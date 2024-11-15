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

    def process_creative(self, creative: Dict) -> Dict:
        """Process creative information into a flat structure"""
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
        
        platform_custom = creative.get('platform_customizations', {})
        if platform_custom:
            for platform, settings in platform_custom.items():
                processed[f'{platform}_customization'] = str(settings)
        
        return processed

    def process_data(
        self,
        ad_data: List[Dict],
        attribution_window: str = "default"
    ) -> pd.DataFrame:
        """
        Process ad data with insights and creative details.
        
        Args:
            ad_data: List of ad dictionaries
            attribution_window: Attribution window to use
        """
        processed_data = []

        for ad in ad_data:
            ad_id = ad["id"]
            
            # Fetch creative details
            creative_id = ad.get("creative", {}).get("id")
            creative_details = self.fetch_creative_details(creative_id) if creative_id else {}
            creative_info = self.process_creative(creative_details)
            
            # Fetch insights with different date breakdowns
            daily_insights = self.fetch_ad_insights(
                ad_id,
                attribution_window=attribution_window
            )

            # Process metrics for each day
            for insight in daily_insights:
                metrics = {
                    **self.metric_calculator.calculate_basic_metrics(insight),
                    **self.metric_calculator.calculate_conversion_metrics(insight),
                    **self.metric_calculator.calculate_video_metrics(insight),
                    **self.metric_calculator.calculate_engagement_metrics(insight)
                }

                processed_insight = {
                    "ad_id": ad_id,
                    "ad_name": ad.get("name"),
                    "adset_id": ad.get("adset_id"),
                    "campaign_id": ad.get("campaign_id"),
                    "status": ad.get("status"),
                    "date": insight.get("date_start"),
                    **creative_info,
                    **metrics
                }
                processed_data.append(processed_insight)

        return pd.DataFrame(processed_data)

    def fetch_ad_insights(
        self,
        ad_id: str,
        attribution_window: str = "default"
    ) -> List[Dict]:
        """
        Fetch ad performance metrics.
        
        Args:
            ad_id: Ad identifier
            attribution_window: Attribution window to use
        """
        fields = (
            COMMON_METRICS +
            CONVERSION_METRICS +
            VIDEO_METRICS +
            ENGAGEMENT_METRICS
        )

        return self.api_client.get_insights(
            ad_id,
            fields,
            attribution_window,
            "ad"
        )

    def get_ad_performance(
        self,
        date_ranges: List[str] = ["7_days", "28_days", "lifetime"],
        attribution_windows: List[str] = ["1d_click", "7d_click", "default"]
    ) -> Dict[str, pd.DataFrame]:
        """
        Get comprehensive ad performance data.
        
        Args:
            date_ranges: List of date range identifiers
            attribution_windows: List of attribution window identifiers
        """
        return self.get_performance_data(date_ranges, attribution_windows)

    def export_ad_data(
        self,
        data: Dict[str, pd.DataFrame],
        output_dir: str
    ) -> None:
        """
        Export ad data to CSV files.
        
        Args:
            data: Dictionary of DataFrames to export
            output_dir: Directory to export to
        """
        self.export_data(data, output_dir, "ad")