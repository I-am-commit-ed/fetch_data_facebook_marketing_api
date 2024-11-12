from typing import Dict, List, Any
from datetime import datetime, timedelta
import pandas as pd
from ..utils.api_client import MetaAdsAPIClient
from ..utils.metric_calculator import MetricCalculator
from ..config import (
    CAMPAIGN_FIELDS,
    COMMON_METRICS,
    CONVERSION_METRICS,
    ATTRIBUTION_WINDOWS,
    BREAKDOWNS
)

class CampaignFetcher:
    def __init__(self, api_client: MetaAdsAPIClient):
        self.api_client = api_client
        self.metric_calculator = MetricCalculator()

    def fetch_campaigns(self) -> List[Dict]:
        """Fetch basic campaign information"""
        params = {
            "fields": ",".join(CAMPAIGN_FIELDS),
            "limit": 500
        }
        
        return self.api_client.make_request(
            f"act_{self.api_client.account_id}/campaigns",
            params
        )

    def fetch_campaign_insights(
        self,
        campaign_id: str,
        date_preset: str = None,
        time_range: Dict = None,
        attribution_window: str = "default",
        breakdowns: List[str] = None
    ) -> List[Dict]:
        """Fetch campaign performance metrics"""
        fields = COMMON_METRICS + CONVERSION_METRICS + [
            "campaign_name",
            "objective",
            "buying_type"
        ]

        params = {
            **ATTRIBUTION_WINDOWS[attribution_window],
            "level": "campaign"
        }

        if breakdowns:
            params["breakdowns"] = ",".join(breakdowns)

        if date_preset:
            params["date_preset"] = date_preset
        elif time_range:
            params["time_range"] = time_range

        return self.api_client.get_insights(campaign_id, fields, params)

    def process_campaign_data(
        self,
        campaign_data: List[Dict],
        attribution_window: str = "default"
    ) -> pd.DataFrame:
        """Process campaign data with insights"""
        processed_data = []

        for campaign in campaign_data:
            campaign_id = campaign["id"]
            
            # Fetch insights with different date breakdowns
            daily_insights = self.fetch_campaign_insights(
                campaign_id,
                time_range={"since": "2021-01-01", "until": datetime.now().strftime("%Y-%m-%d")},
                attribution_window=attribution_window
            )

            # Process metrics for each day
            for insight in daily_insights:
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
                    **metrics,
                    **{k: insight.get(k, 0) for k in COMMON_METRICS + CONVERSION_METRICS}
                }
                processed_data.append(processed_insight)

        return pd.DataFrame(processed_data)

    def get_campaign_performance(
        self,
        date_ranges: List[str] = ["7_days", "28_days", "lifetime"],
        attribution_windows: List[str] = ["1d_click", "7d_click", "default"]
    ) -> Dict[str, pd.DataFrame]:
        """Get comprehensive campaign performance data"""
        performance_data = {}
        
        # Fetch basic campaign data
        campaigns = self.fetch_campaigns()
        
        # Get performance data for each date range and attribution window
        for date_range in date_ranges:
            for attribution in attribution_windows:
                key = f"{date_range}_{attribution}"
                df = self.process_campaign_data(campaigns, attribution)
                
                if date_range != "lifetime":
                    days = int(date_range.split("_")[0])
                    df = df[df["date"] >= (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")]
                
                performance_data[key] = df

        return performance_data

    def export_campaign_data(self, data: Dict[str, pd.DataFrame], output_dir: str):
        """Export campaign data to CSV files"""
        for key, df in data.items():
            filename = f"campaign_performance_{key}.csv"
            df.to_csv(f"{output_dir}/{filename}", index=False)
            print(f"Exported {filename}")

        # Export aggregated views
        daily_df = data.get("lifetime_default")
        if daily_df is not None:
            # Weekly aggregation
            weekly_df = daily_df.groupby([
                pd.Grouper(key="date", freq="W"),
                "campaign_id",
                "campaign_name"
            ]).sum().reset_index()
            weekly_df.to_csv(f"{output_dir}/campaign_performance_weekly.csv", index=False)

            # Monthly aggregation
            monthly_df = daily_df.groupby([
                pd.Grouper(key="date", freq="M"),
                "campaign_id",
                "campaign_name"
            ]).sum().reset_index()
            monthly_df.to_csv(f"{output_dir}/campaign_performance_monthly.csv", index=False)
