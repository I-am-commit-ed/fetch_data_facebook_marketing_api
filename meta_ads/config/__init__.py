"""
Configuration module for Meta Ads Fetcher.
Import all configurations here for easy access.
"""

from .api_config import (
    API_VERSION,
    BASE_URL,
    DEFAULT_PAGE_SIZE,
    DATE_RANGES,
    ATTRIBUTION_WINDOWS,
    COMMON_METRICS,
    CONVERSION_METRICS,
    VIDEO_METRICS,
    ENGAGEMENT_METRICS
)

from .paths import (
    ROOT_DIR,
    PROJECT_DIR,
    EXPORT_DIR,
    CAMPAIGN_EXPORT_DIR,
    ADSET_EXPORT_DIR,
    AD_EXPORT_DIR
)

__all__ = [
    'API_VERSION',
    'BASE_URL',
    'DEFAULT_PAGE_SIZE',
    'DATE_RANGES',
    'ATTRIBUTION_WINDOWS',
    'COMMON_METRICS',
    'CONVERSION_METRICS',
    'VIDEO_METRICS',
    'ENGAGEMENT_METRICS',
    'ROOT_DIR',
    'PROJECT_DIR',
    'EXPORT_DIR',
    'CAMPAIGN_EXPORT_DIR',
    'ADSET_EXPORT_DIR',
    'AD_EXPORT_DIR'
]