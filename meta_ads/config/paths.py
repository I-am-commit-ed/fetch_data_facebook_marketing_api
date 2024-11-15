"""
Path configurations for Meta Ads Fetcher
"""

from pathlib import Path

# Base directories
ROOT_DIR = Path(__file__).parent.parent.parent
PROJECT_DIR = ROOT_DIR / 'meta_ads'
EXPORT_DIR = PROJECT_DIR / 'exports'

# Export directories for different data types
CAMPAIGN_EXPORT_DIR = EXPORT_DIR / 'campaigns'
ADSET_EXPORT_DIR = EXPORT_DIR / 'adsets'
AD_EXPORT_DIR = EXPORT_DIR / 'ads'

# Create directories if they don't exist
for directory in [EXPORT_DIR, CAMPAIGN_EXPORT_DIR, ADSET_EXPORT_DIR, AD_EXPORT_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# File naming templates
FILE_TEMPLATES = {
    'campaign': 'campaign_data_{date}_{window}.csv',
    'adset': 'adset_data_{date}_{window}.csv',
    'ad': 'ad_data_{date}_{window}.csv',
}

# Export file types
FILE_TYPES = {
    'campaigns': ['daily', 'weekly', 'monthly'],
    'adsets': ['daily', 'weekly', 'monthly'],
    'ads': ['daily', 'weekly', 'monthly']
}

def get_export_path(data_type: str, date_range: str, attribution_window: str) -> Path:
    """
    Get the export path for a specific data type and parameters
    
    Args:
        data_type (str): Type of data (campaign, adset, or ad)
        date_range (str): Date range identifier
        attribution_window (str): Attribution window identifier
    
    Returns:
        Path: Complete path for the export file
    """
    filename = FILE_TEMPLATES[data_type].format(
        date=date_range,
        window=attribution_window
    )
    
    if data_type == 'campaign':
        return CAMPAIGN_EXPORT_DIR / filename
    elif data_type == 'adset':
        return ADSET_EXPORT_DIR / filename
    elif data_type == 'ad':
        return AD_EXPORT_DIR / filename
    else:
        raise ValueError(f"Unknown data type: {data_type}")