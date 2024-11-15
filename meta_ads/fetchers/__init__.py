"""
Fetcher module for Meta Ads data retrieval.
Provides classes for fetching campaign, ad set, and ad data.
"""

from .base import BaseFetcher
from .campaign_fetcher import CampaignFetcher
from .adset_fetcher import AdSetFetcher
from .ad_fetcher import AdFetcher

__all__ = [
    'BaseFetcher',
    'CampaignFetcher',
    'AdSetFetcher',
    'AdFetcher'
]