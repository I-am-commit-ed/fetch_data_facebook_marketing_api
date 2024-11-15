"""
Metric calculator for processing Meta Ads data.
Provides methods for calculating various performance metrics.
"""

from typing import Dict, List, Any
from datetime import datetime, timedelta

class MetricCalculator:
    @staticmethod
    def calculate_basic_metrics(data: Dict[str, Any]) -> Dict[str, float]:
        """
        Calculate basic metrics from raw data.
        
        Args:
            data: Raw metric data from API
            
        Returns:
            Dictionary of calculated basic metrics
        """
        metrics = {}
        
        # Frequency
        impressions = float(data.get('impressions', 0))
        reach = float(data.get('reach', 0))
        metrics['frequency'] = impressions / reach if reach > 0 else 0
        
        # CTR
        clicks = float(data.get('clicks', 0))
        metrics['ctr'] = (clicks / impressions * 100) if impressions > 0 else 0
        
        # CPC
        spend = float(data.get('spend', 0))
        metrics['cpc'] = spend / clicks if clicks > 0 else 0
        
        # CPM
        metrics['cpm'] = (spend / impressions * 1000) if impressions > 0 else 0
        
        return metrics

    @staticmethod
    def calculate_conversion_metrics(data: Dict[str, Any]) -> Dict[str, float]:
        """
        Calculate conversion-related metrics.
        
        Args:
            data: Raw metric data from API
            
        Returns:
            Dictionary of calculated conversion metrics
        """
        metrics = {}
        spend = float(data.get('spend', 0))
        
        # Get conversion values
        actions = data.get('actions', [])
        action_values = {
            action['action_type']: float(action['value'])
            for action in actions
        }
        
        purchases = action_values.get('purchase', 0)
        adds_to_cart = action_values.get('add_to_cart', 0)
        checkouts = action_values.get('initiate_checkout', 0)
        
        # Calculate conversion rates
        impressions = float(data.get('impressions', 0))
        if impressions > 0:
            metrics['purchase_rate'] = (purchases / impressions) * 100
            metrics['add_to_cart_rate'] = (adds_to_cart / impressions) * 100
            metrics['checkout_rate'] = (checkouts / impressions) * 100
        
        # Calculate costs
        if spend > 0:
            metrics['cost_per_purchase'] = spend / purchases if purchases > 0 else 0
            metrics['cost_per_add_to_cart'] = spend / adds_to_cart if adds_to_cart > 0 else 0
            metrics['cost_per_checkout'] = spend / checkouts if checkouts > 0 else 0
        
        # ROAS calculation
        purchase_value = float(data.get('action_values', {}).get('purchase', 0))
        metrics['roas'] = purchase_value / spend if spend > 0 else 0
        
        return metrics

    @staticmethod
    def calculate_video_metrics(data: Dict[str, Any]) -> Dict[str, float]:
        """
        Calculate video-specific metrics.
        
        Args:
            data: Raw metric data from API
            
        Returns:
            Dictionary of calculated video metrics
        """
        metrics = {}
        
        video_views = float(data.get('video_plays', 0))
        impressions = float(data.get('impressions', 0))
        spend = float(data.get('spend', 0))
        
        # View rate
        metrics['view_rate'] = (video_views / impressions * 100) if impressions > 0 else 0
        
        # Cost per video view
        metrics['cost_per_video_view'] = spend / video_views if video_views > 0 else 0
        
        # Video completion rates
        for percentage in [25, 50, 75, 95, 100]:
            views_at_percentage = float(data.get(f'video_plays_at_{percentage}_percent', 0))
            metrics[f'video_completion_rate_{percentage}'] = (
                views_at_percentage / video_views * 100
            ) if video_views > 0 else 0
        
        return metrics

    @staticmethod
    def calculate_engagement_metrics(data: Dict[str, Any]) -> Dict[str, float]:
        """
        Calculate engagement metrics.
        
        Args:
            data: Raw metric data from API
            
        Returns:
            Dictionary of calculated engagement metrics
        """
        metrics = {}
        impressions = float(data.get('impressions', 0))
        
        engagement_types = [
            'post_engagement',
            'post_reactions',
            'post_comments',
            'post_shares',
            'page_engagement'
        ]
        
        for engagement_type in engagement_types:
            value = float(data.get(engagement_type, 0))
            metrics[f'{engagement_type}_rate'] = (
                value / impressions * 100
            ) if impressions > 0 else 0
        
        return metrics

    @staticmethod
    def aggregate_metrics(metrics_list: List[Dict[str, float]]) -> Dict[str, float]:
        """
        Aggregate metrics across multiple periods.
        
        Args:
            metrics_list: List of metric dictionaries to aggregate
            
        Returns:
            Dictionary of aggregated metrics
        """
        aggregated = {}
        count = len(metrics_list)
        
        if count == 0:
            return {}
        
        # Sum metrics
        for metrics in metrics_list:
            for key, value in metrics.items():
                if isinstance(value, (int, float)):
                    aggregated[key] = aggregated.get(key, 0) + value
        
        # Average rate metrics
        rate_metrics = [
            'frequency', 'ctr', 'view_rate', 'purchase_rate',
            'add_to_cart_rate', 'checkout_rate'
        ]
        
        for metric in rate_metrics:
            if metric in aggregated:
                aggregated[metric] = aggregated[metric] / count
        
        return aggregated

    @staticmethod
    def calculate_period_over_period_changes(
        current_period: Dict[str, float],
        previous_period: Dict[str, float]
    ) -> Dict[str, float]:
        """
        Calculate changes between two periods.
        
        Args:
            current_period: Current period metrics
            previous_period: Previous period metrics
            
        Returns:
            Dictionary of metric changes
        """
        changes = {}
        
        for metric, current_value in current_period.items():
            previous_value = previous_period.get(metric, 0)
            if previous_value != 0:
                change = ((current_value - previous_value) / previous_value) * 100
                changes[f'{metric}_change'] = change
            else:
                changes[f'{metric}_change'] = 0 if current_value == 0 else 100
        
        return changes