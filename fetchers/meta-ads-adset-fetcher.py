from typing import Dict, List, Any
from datetime import datetime, timedelta
import pandas as pd
from ..utils.api_client import MetaAdsAPIClient
from ..utils.metric_calculator import MetricCalculator
from ..config import (
    ADSET_FIELDS,
    COMMON_METRICS,
    CONVERSION_METRICS,
    ATTRIBUTION_WINDOWS,
    BREAKDOWNS
)

class AdSetFetcher:
    def __init__(self, api_client: MetaAdsAPIClient):
        self.api_client = api_client
        self.metric_calculator = MetricCalculator()

    def fetch_adsets(self) -> List[Dict]:
        """Fetch basic ad set information"""
        params = {
            "fields": ",".join(ADSET_FIELDS),
            "limit": 500
        }
        
        return self.api_client.make_request(
            f"act_{self.api_client.account_id}/adsets",
            params
        )

    def process_targeting(self, targeting: Dict) -> Dict:
        """Process targeting information into a flat structure"""
        processed = {
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
        return processed

    def fetch_adset_insights(
        self,
        adset_id: str,
        date_preset: str = None,
        time_range: Dict = None,
        attribution_window: str = "default",
        breakdowns: List[str] = None
    ) -> List[Dict]:
        """Fetch ad set performance metrics"""
        fields = COMMON_METRICS + CONVERSION_METRICS + [
            "adset_name",
            "campaign_id",
            "optimization_goal",
            "billing_event"
        ]

        params = {
            **ATTRIBUTION_WINDOWS[attribution_window],
            "level": "adset"
        }

        if breakdowns:
            params["breakdowns"] = ",".join(breakdowns)

        if date_preset:
            params["date_preset"] = date_preset
        elif time_range:
            params["time_range"] = time_range

        return self.api_client.get_insights(adset_id, fields, params)

    def process_adset_data(
        self,
        adset_data: List[Dict],
        attribution_window: str = "default"
    ) -> pd.DataFrame:
        """Process ad set data with insights and targeting"""
        processed_data = []

        for adset in adset_data:
            adset_id = adset["id"]
            targeting = self.process_targeting(adset.get("targeting", {}))
            
            # Fetch insights with different date breakdowns
            daily_insights = self.fetch_adset_insights(
                adset_id,
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
                    "adset_id": adset_id,
                    "adset_name": adset.get("name"),
                    "campaign_id": adset.get("campaign_id"),
                    "status": adset.get("status"),
                    "optimization_goal": adset.get("optimization_goal"),
                    "billing_event": adset.get("billing_event"),
                    "bid_amount": adset.get("bid_amount"),
                    "date": insight.get("date_start"),
                    **targeting,
                    **metrics,
                    **{k: insight.get(k, 0) for k in COMMON_METRICS + CONVERSION_METRICS}
                }
                processed_data.append(processed_insight)

        return pd.DataFrame(processed_data)

    def get_adset_performance(
        self,
        date_ranges: List[str] = ["7_days", "28_days", "lifetime"],
        attribution_windows: List[str] = ["1d_click", "7d_click", "default"]
    ) -> Dict[str, pd.DataFrame]:
        """Get comprehensive ad set performance data"""
        performance_data = {}
        
        # Fetch basic ad set data
        adsets = self.fetch_adsets()
        
        # Get performance data for each date range and attribution window
        for date_range in date_ranges:
            for attribution in attribution_windows:
                key = f"{date_range}_{attribution}"
                df = self.process_adset_data(adsets, attribution)
                
                if date_range != "lifetime":
                    days = int(date_range.split("_")[0])
                    df = df[df["date"] >= (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")]
                
                performance_data[key] = df

        return performance_data

    def export_adset_data(self, data: Dict[str, pd.DataFrame], output_dir: str):
        """Export ad set data to CSV files"""
        for key, df in data.items():
            filename = f"adset_performance_{key}.csv"
            df.to_csv(f"{output_dir}/{filename}", index=False)
            print(f"Exported {filename}")

        # Export aggregated views
        daily_df = data.get("lifetime_default")
        if daily_df is not None:
            # Weekly aggregation
            weekly_df = daily_df.groupby([
                pd.Grouper(key="date", freq="W"),
                "adset_id",
                "adset_name",
                "campaign_id"
            ]).sum().reset_index()
            weekly_df.to_csv(f"{output_dir}/adset_performance_weekly.csv", index=False)

            # Monthly aggregation
            monthly_df = daily_df.groupby([
                pd.Grouper(key="date", freq="M"),
                "adset_id",
                "adset_name",
                "campaign_id"
            ]).sum().reset_index()
            monthly_df.to_csv(f"{output_dir}/adset_performance_monthly.csv", index=False)
