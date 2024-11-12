from typing import Dict, List, Any
from datetime import datetime, timedelta
import pandas as pd
from ..utils.api_client import MetaAdsAPIClient
from ..utils.metric_calculator import MetricCalculator
from ..config import (
    AD_FIELDS,
    CREATIVE_FIELDS,
    COMMON_METRICS,
    CONVERSION_METRICS,
    VIDEO_METRICS,
    ENGAGEMENT_METRICS,
    ATTRIBUTION_WINDOWS,
    BREAKDOWNS
)

class AdFetcher:
    def __init__(self, api_client: MetaAdsAPIClient):
        self.api_client = api_client
        self.metric_calculator = MetricCalculator()

    def fetch_ads(self) -> List[Dict]:
        """Fetch basic ad information"""
        params = {
            "fields": ",".join(AD_FIELDS),
            "limit": 500
        }
        
        return self.api_client.make_request(
            f"act_{self.api_client.account_id}/ads",
            params
        )

    def fetch_creative_details(self, creative_id: str) -> Dict:
        """Fetch detailed creative information"""
        params = {
            "fields": ",".join(CREATIVE_FIELDS)
        }
        
        return self.api_client.make_request(creative_id, params)

    def fetch_ad_insights(
        self,
        ad_id: str,
        date_preset: str = None,
        time_range: Dict = None,
        attribution_window: str = "default",
        breakdowns: List[str] = None
    ) -> List[Dict]:
        """Fetch ad performance metrics"""
        fields = (
            COMMON_METRICS +
            CONVERSION_METRICS +
            VIDEO_METRICS +
            ENGAGEMENT_METRICS +
            [
                "ad_name",
                "adset_id",
                "campaign_id"
            ]
        )

        params = {
            **ATTRIBUTION_WINDOWS[attribution_window],
            "level": "ad"
        }

        if breakdowns:
            params["breakdowns"] = ",".join(breakdowns)

        if date_preset:
            params["date_preset"] = date_preset
        elif time_range:
            params["time_range"] = time_range

        return self.api_client.get_insights(ad_id, fields, params)

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
        
        # Process platform customizations
        platform_custom = creative.get('platform_customizations', {})
        if platform_custom:
            for platform, settings in platform_custom.items():
                processed[f'{platform}_customization'] = str(settings)
        
        return processed

    def process_ad_data(
        self,
        ad_data: List[Dict],
        attribution_window: str = "default"
    ) -> pd.DataFrame:
        """Process ad data with insights and creative details"""
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
                time_range={"since": "2021-01-01", "until": datetime.now().strftime("%Y-%m-%d")},
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
                    **metrics,
                    **{k: insight.get(k, 0) for k in COMMON_METRICS + CONVERSION_METRICS + VIDEO_METRICS + ENGAGEMENT_METRICS}
                }
                processed_data.append(processed_insight)

        return pd.DataFrame(processed_data)

    def get_ad_performance(
        self,
        date_ranges: List[str] = ["7_days", "28_days", "lifetime"],
        attribution_windows: List[str] = ["1d_click", "7d_click", "default"],
        include_video_metrics: bool = True,
        include_engagement_metrics: bool = True
    ) -> Dict[str, pd.DataFrame]:
        """Get comprehensive ad performance data"""
        performance_data = {}
        
        # Fetch basic ad data
        ads = self.fetch_ads()
        
        # Get performance data for each date range and attribution window
        for date_range in date_ranges:
            for attribution in attribution_windows:
                key = f"{date_range}_{attribution}"
                df = self.process_ad_data(ads, attribution)
                
                if date_range != "lifetime":
                    days = int(date_range.split("_")[0])
                    df = df[df["date"] >= (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")]
                
                performance_data[key] = df

        return performance_data

    def export_ad_data(self, data: Dict[str, pd.DataFrame], output_dir: str):
        """Export ad data to CSV files"""
        for key, df in data.items():
            filename = f"ad_performance_{key}.csv"
            df.to_csv(f"{output_dir}/{filename}", index=False)
            print(f"Exported {filename}")

        # Export aggregated views
        daily_df = data.get("lifetime_default")
        if daily_df is not None:
            # Weekly aggregation
            weekly_df = daily_df.groupby([
                pd.Grouper(key="date", freq="W"),
                "ad_id",
                "ad_name",
                "adset_id",
                "campaign_id"
            ]).sum().reset_index()
            weekly_df.to_csv(f"{output_dir}/ad_performance_weekly.csv", index=False)

            # Monthly aggregation
            monthly_df = daily_df.groupby([
                pd.Grouper(key="date", freq="M"),
                "ad_id",
                "ad_name",
                "adset_id",
                "campaign_id"
            ]).sum().reset_index()
            monthly_df.to_csv(f"{output_dir}/ad_performance_monthly.csv", index=False)
