"""
API Client for interacting with Meta Ads API.
Handles authentication, requests, rate limiting, and error handling.
"""

import requests
import time
import json
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import logging
from functools import wraps

from ..config.api_config import (
    BASE_URL,
    DEFAULT_PAGE_SIZE,
    ATTRIBUTION_WINDOWS,
    COMMON_METRICS,
    CONVERSION_METRICS
)

def retry_with_backoff(max_retries: int = 3, initial_delay: float = 5.0):
    """Retry decorator with exponential backoff"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None
            
            for retry in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if "rate limit" in str(e).lower():
                        sleep_time = delay * (2 ** retry)
                        logging.warning(f"Rate limit hit. Retrying in {sleep_time} seconds...")
                        time.sleep(sleep_time)
                        continue
                    raise e
            
            raise last_exception
        return wrapper
    return decorator

class MetaAdsAPIClient:
    def __init__(self, access_token: str, account_id: str):
        """
        Initialize the Meta Ads API client.
        
        Args:
            access_token: Meta Ads API access token
            account_id: Meta Ads account ID
        """
        self.access_token = access_token
        self.account_id = account_id.replace('act_', '')
        self.rate_limit_remaining = 100
        self.last_request_time = 0
        self.setup_logging()

    def setup_logging(self):
        """Configure logging for the API client"""
        self.logger = logging.getLogger('meta_ads_api')
        self.logger.setLevel(logging.INFO)
        
        # Create handlers if they don't exist
        if not self.logger.handlers:
            # Console handler
            console_handler = logging.StreamHandler()
            console_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            console_handler.setFormatter(console_format)
            self.logger.addHandler(console_handler)
            
            # File handler
            file_handler = logging.FileHandler('meta_ads_api.log')
            file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(file_format)
            self.logger.addHandler(file_handler)

    def _handle_rate_limiting(self) -> None:
        """Enhanced rate limiting handler with exponential backoff"""
        current_time = time.time()
        
        # Calculate wait time with exponential backoff
        if not hasattr(self, '_retry_count'):
            self._retry_count = 0
        
        base_wait_time = 5.0
        wait_time = base_wait_time * (2 ** self._retry_count)
        max_wait_time = 300  # 5 minutes maximum
        
        wait_time = min(wait_time, max_wait_time)
        
        if current_time - self.last_request_time < wait_time:
            sleep_time = wait_time - (current_time - self.last_request_time)
            self.logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
            self._retry_count += 1
        else:
            self._retry_count = 0
        
        self.last_request_time = time.time()

    def _mask_sensitive_data(self, text: str) -> str:
        """Mask sensitive information like access tokens"""
        if self.access_token in text:
            return text.replace(self.access_token, '****')
        return text

    @retry_with_backoff()
    def make_request(
        self, 
        endpoint: str, 
        params: Dict[str, Any], 
        method: str = "GET"
    ) -> List[Dict]:
        """
        Make API request with error handling and pagination.
        
        Args:
            endpoint: API endpoint to call
            params: Query parameters
            method: HTTP method to use
            
        Returns:
            List of response data dictionaries
            
        Raises:
            requests.exceptions.RequestException: If API request fails
        """
        self._handle_rate_limiting()
        
        # Create a copy of params for logging (with masked sensitive data)
        safe_params = params.copy()
        if 'access_token' in safe_params:
            safe_params['access_token'] = '****'
        
        self.logger.info(f"Making {method} request to {endpoint}")
        self.logger.debug(f"Parameters: {safe_params}")
        
        try:
            params["access_token"] = self.access_token
            url = f"{BASE_URL}/{endpoint}"
            
            if method == "GET":
                response = requests.get(url=url, params=params, timeout=30)
            elif method == "POST":
                response = requests.post(url=url, json=params, timeout=30)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            # Handle rate limiting errors
            if response.status_code == 429 or "rate limit" in response.text.lower():
                raise Exception("Rate limit reached")

            response.raise_for_status()
            
            data = response.json()
            
            # Handle pagination
            all_data = data.get('data', [])
            paging = data.get('paging', {})
            
            while 'next' in paging:
                self.logger.debug("Fetching next page of results")
                self._handle_rate_limiting()
                response = requests.get(paging['next'], timeout=30)
                response.raise_for_status()
                
                next_data = response.json()
                all_data.extend(next_data.get('data', []))
                paging = next_data.get('paging', {})
            
            self.logger.info(f"Successfully retrieved {len(all_data)} records")
            return all_data
            
        except requests.exceptions.RequestException as e:
            error_msg = self._mask_sensitive_data(str(e))
            self.logger.error(f"API Error: {error_msg}")
            
            if hasattr(e, 'response') and e.response is not None:
                response_text = self._mask_sensitive_data(e.response.text)
                self.logger.error(f"Response: {response_text}")
            
            raise

    def get_insights(
        self,
        object_id: str,
        fields: List[str],
        attribution_window: str,
        level: str
    ) -> List[Dict]:
        """Get insights with date range filtering"""
        params = {
            "fields": ",".join(fields),
            "level": level,
            "time_range": {"since": "2024-01-01", "until": "2024-11-17"},  # Adjust date range as needed
            "limit": 100  # Reduced from 500 to avoid rate limits
        }
        
        if attribution_window in ATTRIBUTION_WINDOWS:
            params.update(ATTRIBUTION_WINDOWS[attribution_window])
        
        endpoint = f"{object_id}/insights"
        return self.make_request(endpoint, params)

    def batch_request(
        self,
        requests: List[Dict[str, Any]],
        batch_size: int = 50
    ) -> List[Dict]:
        """
        Make batch request to the API.
        
        Args:
            requests: List of request specifications
            batch_size: Maximum number of requests per batch
            
        Returns:
            List of response data dictionaries
        """
        self.logger.info(f"Making batch request with {len(requests)} requests")
        
        results = []
        for i in range(0, len(requests), batch_size):
            batch = requests[i:i + batch_size]
            batch_params = {
                "batch": json.dumps(batch),
                "include_headers": "false"
            }
            
            batch_results = self.make_request("", batch_params, method="POST")
            results.extend(batch_results)
            
            # Add delay between batches
            if i + batch_size < len(requests):
                time.sleep(2)
        
        return results

    def validate_access(self) -> bool:
        """
        Validate API access and credentials.
        
        Returns:
            bool: True if credentials are valid
        """
        try:
            self.logger.info("Validating API access")
            endpoint = f"act_{self.account_id}"
            params = {"fields": "name,account_status,business_name,currency,timezone_name"}
            
            response = self.make_request(endpoint, params)
            self.logger.info("API access validation successful")
            return True
            
        except Exception as e:
            self.logger.error(f"API access validation failed: {str(e)}")
            return False

    def get_account_info(self) -> Dict:
        """
        Get basic information about the ad account.
        
        Returns:
            Dictionary containing account information
        """
        try:
            self.logger.info("Fetching account information")
            endpoint = f"act_{self.account_id}"
            params = {
                "fields": [
                    "name",
                    "account_status",
                    "business_name",
                    "currency",
                    "timezone_name",
                    "spend_cap",
                    "amount_spent",
                    "balance"
                ]
            }
            
            response = self.make_request(endpoint, params)
            self.logger.info("Successfully retrieved account information")
            return response[0] if response else {}
            
        except Exception as e:
            self.logger.error(f"Failed to fetch account information: {str(e)}")
            raise