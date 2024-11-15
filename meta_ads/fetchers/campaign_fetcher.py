"""
Campaign data fetcher for Meta Ads API.
"""

from typing import Dict, List, Any
import pandas as pd

from .base import BaseFetcher
from ..config.api_config import (
    CAMPAIGN_FIELDS,
    COMMON_METRICS,
    CONVERSION_METRICS,
    ATTRIBUTION_WINDOWS
)

class CampaignFetcher(BaseFetcher):
    def fetch_data(self) -> List[Dict]:
        """Fetch basic campaign information"""
        params = {
            "fields": ",".join(CAMPAIGN_FIELDS),
            "limit": 500
        }
        
        endpoint = f"act_{self.api_client.account_id}/campaigns"
        return self.api_client.make_request(endpoint, params)

    def fetch_campaign_insights(
        self,
        campaign_id: str,
        attribution_window: str = "default"
    ) -> List[Dict]:
        """
        Fetch campaign performance metrics.
        
        Args:
            campaign_id: Campaign identifier
            attribution_window: Attribution window to use
        """
        fields = COMMON_METRICS + [
            "campaign_name",
            "objective",
            "buying_type"
        ]

        return self.api_client.get_insights(
            campaign_id,
            fields,
            attribution_window,
            "campaign"
        )

    def process_data(
        self,
        campaign_data: List[Dict],
        attribution_window: str = "default"
    ) -> pd.DataFrame:
        """
        Process campaign data with insights.
        
        Args:
            campaign_data: List of campaign dictionaries
            attribution_window: Attribution window to use
        """
        processed_data = []

        for campaign in campaign_data:
            campaign_id = campaign["id"]
            
            # Fetch insights
            insights = self.fetch_campaign_insights(campaign_id, attribution_window)

            # Process metrics for each day
            for insight in insights:
                metrics = {
                    **self.metric_calculator.calculate_basic_metrics(insight),
                    **self.metric_calculator.calculate_conversion_metrics(insight)
                }

                processed_insight = {
                    "campaign_id": campaign_id,
                    "campaign_name": campaign.get("name"),
                    "objective": campaign.get("objective"),
                    "buying_type": campaign.get("buying_type"),
                    "status": campaign.get("status"),
                    "date": insight.get("date_start"),
                    **metrics
                }
                processed_data.append(processed_insight)

        return pd.DataFrame(processed_data)

    def get_campaign_performance(
        self,
        date_ranges: List[str] = ["7_days", "28_days", "lifetime"],
        attribution_windows: List[str] = ["1d_click", "7d_click", "default"]
    ) -> Dict[str, pd.DataFrame]:
        """
        Get comprehensive campaign performance data.
        
        Args:
            date_ranges: List of date range identifiers
            attribution_windows: List of attribution window identifiers
        """
        return self.get_performance_data(date_ranges, attribution_windows)

    def export_campaign_data(
        self,
        data: Dict[str, pd.DataFrame],
        output_dir: str
    ) -> None:
        """
        Export campaign data to CSV files.
        
        Args:
            data: Dictionary of DataFrames to export
            output_dir: Directory to export to
        """
        self.export_data(data, output_dir, "campaign")