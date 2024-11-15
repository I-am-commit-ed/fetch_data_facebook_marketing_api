"""
Base fetcher class providing common functionality for all fetchers.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any
from datetime import datetime
import pandas as pd

from ..utils.api_client import MetaAdsAPIClient
from ..utils.metrics import MetricCalculator
from ..config.paths import get_export_path

class BaseFetcher(ABC):
    def __init__(self, api_client: MetaAdsAPIClient):
        self.api_client = api_client
        self.metric_calculator = MetricCalculator()

    @abstractmethod
    def fetch_data(self) -> List[Dict]:
        """
        Fetch basic entity information.
        Must be implemented by child classes.
        """
        pass

    @abstractmethod
    def process_data(self, data: List[Dict], attribution_window: str = "default") -> pd.DataFrame:
        """
        Process fetched data.
        Must be implemented by child classes.
        """
        pass

    def get_performance_data(
        self,
        date_ranges: List[str] = ["7_days", "28_days", "lifetime"],
        attribution_windows: List[str] = ["1d_click", "7d_click", "default"]
    ) -> Dict[str, pd.DataFrame]:
        """
        Get performance data for different date ranges and attribution windows.
        
        Args:
            date_ranges: List of date range identifiers
            attribution_windows: List of attribution window identifiers
        
        Returns:
            Dictionary of DataFrames with performance data
        """
        performance_data = {}
        
        # Fetch basic data
        data = self.fetch_data()
        
        # Get performance data for each combination
        for date_range in date_ranges:
            for attribution in attribution_windows:
                key = f"{date_range}_{attribution}"
                df = self.process_data(data, attribution)
                performance_data[key] = df

        return performance_data

    def export_data(
        self,
        data: Dict[str, pd.DataFrame],
        output_dir: str,
        data_type: str
    ) -> None:
        """
        Export data to CSV files.
        
        Args:
            data: Dictionary of DataFrames to export
            output_dir: Directory to export to
            data_type: Type of data being exported (campaign, adset, or ad)
        """
        for key, df in data.items():
            date_range, attribution = key.split('_', 1)
            export_path = get_export_path(data_type, date_range, attribution)
            df.to_csv(export_path, index=False)
            print(f"Exported {data_type} data to {export_path}")

        # Create aggregated views for lifetime data
        if 'lifetime_default' in data:
            self._export_aggregated_views(
                data['lifetime_default'],
                output_dir,
                data_type
            )

    def _export_aggregated_views(
        self,
        df: pd.DataFrame,
        output_dir: str,
        data_type: str
    ) -> None:
        """
        Export aggregated weekly and monthly views.
        
        Args:
            df: DataFrame to aggregate
            output_dir: Directory to export to
            data_type: Type of data being aggregated
        """
        # Ensure date column is datetime
        df['date'] = pd.to_datetime(df['date'])

        # Weekly aggregation
        weekly_df = df.groupby([
            pd.Grouper(key='date', freq='W'),
            f'{data_type}_id',
            f'{data_type}_name'
        ]).sum().reset_index()
        
        weekly_path = get_export_path(data_type, 'weekly', 'aggregated')
        weekly_df.to_csv(weekly_path, index=False)
        print(f"Exported weekly {data_type} data to {weekly_path}")

        # Monthly aggregation
        monthly_df = df.groupby([
            pd.Grouper(key='date', freq='M'),
            f'{data_type}_id',
            f'{data_type}_name'
        ]).sum().reset_index()
        
        monthly_path = get_export_path(data_type, 'monthly', 'aggregated')
        monthly_df.to_csv(monthly_path, index=False)
        print(f"Exported monthly {data_type} data to {monthly_path}")