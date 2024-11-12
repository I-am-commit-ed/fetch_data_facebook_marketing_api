import requests
from typing import Dict, List, Any, Optional
import time
from datetime import datetime, timedelta
import json
from ..config import BASE_URL, DEFAULT_PAGE_SIZE

class MetaAdsAPIClient:
    def __init__(self, access_token: str, account_id: str):
        self.access_token = access_token
        self.account_id = account_id.replace('act_', '')
        self.rate_limit_remaining = 100
        self.last_request_time = 0

    def _handle_rate_limiting(self):
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
        """Make API request with error handling and pagination"""
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
        params: Dict[str, Any],
        level: str = "campaign"
    ) -> List[Dict]:
        """Fetch insights with specific parameters"""
        endpoint = f"{object_id}/insights"
        
        base_params = {
            "level": level,
            "fields": ",".join(fields)
        }
        
        params.update(base_params)
        
        return self.make_request(endpoint, params)

    def batch_request(self, requests: List[Dict[str, Any]]) -> List[Dict]:
        """Make batch request to the API"""
        batch_params = {
            "batch": json.dumps(requests),
            "include_headers": "false"
        }
        
        return self.make_request("", batch_params, method="POST")

    def validate_access(self) -> bool:
        """Validate API access and credentials"""
        try:
            endpoint = f"act_{self.account_id}"
            params = {"fields": "name"}
            
            response = self.make_request(endpoint, params)
            return True
        except:
            return False
