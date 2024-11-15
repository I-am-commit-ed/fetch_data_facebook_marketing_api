"""
Ad Set data fetcher for Meta Ads API.
"""

from typing import Dict, List, Any
import pandas as pd

from .base import BaseFetcher
from ..config.api_config import ADSET_FIELDS, COMMON_METRICS, CONVERSION_METRICS

class AdSetFetcher(BaseFetcher):
    def fetch_data(self) -> List[Dict]:
        """Fetch basic ad set information"""
        params = {
            "fields": ",".join(ADSET_FIELDS),
            "limit": 500
        }
        
        endpoint = f"act_{self.api_client.account_id}/adsets"
        return self.api_client.make_request(endpoint, params)

    def process_targeting(self, targeting: Dict) -> Dict:
        """
        Process targeting information into a flat structure.
        
        Args:
            targeting: Targeting dictionary from API
        """
        return {
            'countries': targeting.get('geo_locations', {}).get('countries', []),
            'age_min': targeting.get('age_min'),
            'age_max': targeting.get('age_max'),
            'genders': targeting.get('genders', []),
            'custom_audiences': [
                audience.get('name') 
                for audience in targeting.get('custom_audiences', [])
            ],
            'excluded_custom_audiences': [
                audience.get('name') 
                for audience in targeting.get('excluded_custom_audiences', [])
            ],
            'publisher_platforms': targeting.get('publisher_platforms', []),
            'facebook_positions': targeting.get('facebook_positions', []),
            'instagram_positions': targeting.get('instagram_positions', []),
            'device_platforms': targeting.get('device_platforms', [])
        }

    def fetch_adset_insights(
        self,
        adset_id: str,
        attribution_window: str = "default"
    ) -> List[Dict]:
        """
        Fetch ad set performance metrics.
        
        Args:
            adset_id: Ad Set identifier
            attribution_window: Attribution window to use
        """
        fields = COMMON_METRICS + CONVERSION_METRICS + [
            "adset_name",
            "campaign_id",
            "optimization_goal",
            "billing_event"
        ]

        return self.api_client.get_insights(
            adset_id,
            fields,
            attribution_window,
            "adset"
        )

    def process_data(
        self,
        adset_data: List[Dict],
        attribution_window: str = "default"
    ) -> pd.DataFrame:
        """
        Process ad set data with insights and targeting.
        
        Args:
            adset_data: List of ad set dictionaries
            attribution_window: Attribution window to use
        """
        processed_data = []

        for adset in adset_data:
            adset_id = adset["id"]
            targeting = self.process_targeting(adset.get("targeting", {}))
            
            # Fetch insights
            insights = self.fetch_adset_insights(adset_id, attribution_window)

            # Process metrics for each day
            for insight in insights:
                metrics = {
                    **self.metric_calculator.calculate_basic_metrics(insight),
                    **self.metric_calculator.calculate_conversion_metrics(insight)
                }

                processed_insight = {
                    "adset_id": adset_id,
                    "adset_name": adset.get("name"),
                    "campaign_id": adset.get("campaign_id"),
                    "status": adset.get("status"),
                    "optimization_goal": adset.get("optimization_goal"),
                    "billing_event": adset.get("billing_event"),
                    "bid_amount": adset.get("bid_amount"),
                    "date": insight.get("date_start"),
                    **targeting,
                    **metrics
                }
                processed_data.append(processed_insight)

        return pd.DataFrame(processed_data)

    def get_adset_performance(
        self,
        date_ranges: List[str] = ["7_days", "28_days", "lifetime"],
        attribution_windows: List[str] = ["1d_click", "7d_click", "default"]
    ) -> Dict[str, pd.DataFrame]:
        """
        Get comprehensive ad set performance data.
        
        Args:
            date_ranges: List of date range identifiers
            attribution_windows: List of attribution window identifiers
        """
        return self.get_performance_data(date_ranges, attribution_windows)

    def export_adset_data(
        self,
        data: Dict[str, pd.DataFrame],
        output_dir: str
    ) -> None:
        """
        Export ad set data to CSV files.
        
        Args:
            data: Dictionary of DataFrames to export
            output_dir: Directory to export to
        """
        self.export_data(data, output_dir, "adset")