"""
API Configuration settings for Meta Ads API
"""

from typing import Dict, List
from datetime import datetime, timedelta

# API Configuration
API_VERSION = "v18.0"
BASE_URL = f"https://graph.facebook.com/{API_VERSION}"
DEFAULT_PAGE_SIZE = 500

# Metrics Configuration
COMMON_METRICS = [
    "spend",
    "impressions",
    "reach",
    "clicks",
    "unique_clicks",
    "inline_link_clicks",
    "unique_inline_link_clicks",
    "cpc",
    "cpm",
    "ctr"
]

CONVERSION_METRICS = [
    "actions",  # This will include purchases, adds_to_cart, etc.
    "action_values",
    "cost_per_action_type",
    "cost_per_unique_action_type",
    "unique_actions",
    "website_purchases",
    "website_adds_to_cart",
    "website_checkouts_initiated"
]

VIDEO_METRICS = [
    "video_p25_watched_actions",
    "video_p50_watched_actions",
    "video_p75_watched_actions",
    "video_p95_watched_actions",
    "video_p100_watched_actions",
    "video_avg_time_watched_actions",
    "video_continuous_2_sec_watched_actions",
    "video_30_sec_watched_actions"
]

ENGAGEMENT_METRICS = [
    "post_engagement",
    "post_reactions",
    "post_comments",
    "post_shares",
    "page_engagement"
]

# Campaign Fields
CAMPAIGN_FIELDS = [
    "id",
    "name",
    "objective",
    "buying_type",
    "status",
    "start_time",
    "stop_time",
    "daily_budget",
    "lifetime_budget",
    "bid_strategy",
    "special_ad_categories"
]

# Ad Set Fields
ADSET_FIELDS = [
    "id",
    "name",
    "campaign_id",
    "status",
    "targeting",
    "optimization_goal",
    "billing_event",
    "bid_amount",
    "budget_remaining",
    "daily_budget",
    "lifetime_budget",
    "attribution_spec",
    "start_time",
    "end_time"
]

# Ad Fields
AD_FIELDS = [
    "id",
    "name",
    "adset_id",
    "campaign_id",
    "status",
    "creative",
    "tracking_specs",
    "conversion_specs",
    "created_time",
    "updated_time"
]

# Creative Fields
CREATIVE_FIELDS = [
    "id",
    "name",
    "title",
    "body",
    "object_story_spec",
    "image_url",
    "video_id",
    "call_to_action_type",
    "link_url",
    "thumbnail_url",
    "image_hash",
    "platform_customizations"
]

# Date Ranges
DATE_RANGES = {
    "1_day": 1,
    "7_days": 7,
    "28_days": 28,
    "90_days": 90,
    "lifetime": None
}

# Attribution Windows
ATTRIBUTION_WINDOWS = {
    "1d_click": {"action_attribution_windows": ["1d_click"]},
    "7d_click": {"action_attribution_windows": ["7d_click"]},
    "28d_click": {"action_attribution_windows": ["28d_click"]},
    "1d_view": {"action_attribution_windows": ["1d_view"]},
    "7d_view": {"action_attribution_windows": ["7d_view"]},
    "default": {"action_attribution_windows": ["7d_click", "1d_view"]}
}

# Breakdowns
BREAKDOWNS = {
    "time": ["day", "week", "month"],
    "demographics": ["age", "gender", "country"],
    "placement": ["publisher_platform", "platform_position"]
}