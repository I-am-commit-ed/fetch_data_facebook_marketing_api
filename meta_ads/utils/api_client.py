"""
API Client for interacting with Meta Ads API.
Handles authentication, requests, and rate limiting.
"""

import requests
import time
import json
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

from ..config.api_config import BASE_URL, DEFAULT_PAGE_SIZE, ATTRIBUTION_WINDOWS

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

    def _handle_rate_limiting(self) -> None:
        """Basic rate limiting handler"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < 1:  # Minimum 1 second between requests
            time.sleep(1 - time_since_last_request)
        
        self.last_request_time = time.time()

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
        
        params["access_token"] = self.access_token
        url = f"{BASE_URL}/{endpoint}"
        
        try:
            if method == "GET":
                response = requests.get(url=url, params=params)
            elif method == "POST":
                response = requests.post(url=url, json=params)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            response.raise_for_status()
            
            data = response.json()
            
            # Handle pagination
            all_data = data.get('data', [])
            paging = data.get('paging', {})
            
            while 'next' in paging:
                self._handle_rate_limiting()
                response = requests.get(paging['next'])
                response.raise_for_status()
                
                next_data = response.json()
                all_data.extend(next_data.get('data', []))
                paging = next_data.get('paging', {})
            
            return all_data
            
        except requests.exceptions.RequestException as e:
            print(f"API Error: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response: {e.response.text}")
            raise

    def get_insights(
        self,
        object_id: str,
        fields: List[str],
        attribution_window: str = "default",
        level: str = "campaign"
    ) -> List[Dict]:
        """
        Fetch insights with specific parameters.
        
        Args:
            object_id: ID of the object to get insights for
            fields: List of fields to retrieve
            attribution_window: Attribution window to use
            level: Data level (campaign, adset, or ad)
            
        Returns:
            List of insight data dictionaries
        """
        endpoint = f"{object_id}/insights"
        
        base_params = {
            "level": level,
            "fields": ",".join(fields),
            **ATTRIBUTION_WINDOWS[attribution_window]
        }
        
        # Add time range for last 90 days
        end_date = datetime.now()
        start_date = end_date - timedelta(days=90)
        
        base_params["time_range"] = {
            "since": start_date.strftime("%Y-%m-%d"),
            "until": end_date.strftime("%Y-%m-%d")
        }
        
        return self.make_request(endpoint, base_params)

    def batch_request(self, requests: List[Dict[str, Any]]) -> List[Dict]:
        """
        Make batch request to the API.
        
        Args:
            requests: List of request specifications
            
        Returns:
            List of response data dictionaries
        """
        batch_params = {
            "batch": json.dumps(requests),
            "include_headers": "false"
        }
        
        return self.make_request("", batch_params, method="POST")

    def validate_access(self) -> bool:
        """
        Validate API access and credentials.
        
        Returns:
            bool: True if credentials are valid
        """
        try:
            endpoint = f"act_{self.account_id}"
            params = {"fields": "name"}
            
            response = self.make_request(endpoint, params)
            return True
        except:
            return False