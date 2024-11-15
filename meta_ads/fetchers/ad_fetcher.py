"""
Ad data fetcher for Meta Ads API.
"""

from typing import Dict, List, Any
import pandas as pd

from .base import BaseFetcher
from ..config.api_config import (
    AD_FIELDS,
    CREATIVE_FIELDS,
    COMMON_METRICS,
    CONVERSION_METRICS,
    VIDEO_METRICS,
    ENGAGEMENT_METRICS
)

class AdFetcher(BaseFetcher):
    def fetch_data(self) -> List[Dict]:
        """Fetch basic ad information"""
        params = {
            "fields": ",".join(AD_FIELDS),
            "limit": 500
        }
        
        endpoint = f"act_{self.api_client.account_id}/ads"
        return self.api_client.make_request(endpoint, params)

    def fetch_creative_details(self, creative_id: str) -> Dict:
        """
        Fetch detailed creative information.
        
        Args:
            creative_id: Creative identifier
        """
        params = {
            "fields": ",".join(CREATIVE_FIELDS)
        }
        
        return self.api_client.make_request(creative_id, params)

    def process_creative(self, creative: Dict) -> Dict:
        """
        Process creative information into a flat structure.
        
        Args:
            creative: Creative dictionary from API
        """
        processed = {
            'creative_id': creative.get('id'),
            'creative_name': creative.get('name'),
            'body': creative.get('body'),
            'title': creative.get('title'),
            'call_to_action_type': creative.get('call_to_action_type'),
            'link_url': creative.get('link_url'),
            'image_url': creative.get('image_url'),
            'video_id': creative.get('video_id')
        }
        
        # Process platform customizations
        platform_custom = creative.