import os
from pathlib import Path
from dotenv import load_dotenv
from meta_ads.utils.api_client import MetaAdsAPIClient
from meta_ads.fetchers.campaign_fetcher import CampaignFetcher
from meta_ads.fetchers.adset_fetcher import AdSetFetcher
from meta_ads.fetchers.ad_fetcher import AdFetcher
from meta_ads.config import (
    EXPORT_DIR,
    CAMPAIGN_EXPORT_DIR,
    ADSET_EXPORT_DIR,
    AD_EXPORT_DIR
)
import time
import json

class MetaAdsDataManager:
    def __init__(self):
        self.setup_environment()
        self.api_client = MetaAdsAPIClient(self.access_token, self.account_id)
        self.campaign_fetcher = CampaignFetcher(self.api_client)
        self.adset_fetcher = AdSetFetcher(self.api_client)
        self.ad_fetcher = AdFetcher(self.api_client)
        
        # Use paths from config
        self.output_dir = EXPORT_DIR
        self.campaign_dir = CAMPAIGN_EXPORT_DIR
        self.adset_dir = ADSET_EXPORT_DIR
        self.ad_dir = AD_EXPORT_DIR

    def setup_environment(self):
        """Setup environment variables"""
        load_dotenv()
        
        self.access_token = os.getenv('FACEBOOK_ACCESS_TOKEN')
        self.account_id = os.getenv('FACEBOOK_AD_ACCOUNT_ID')
        
        if not self.access_token or not self.account_id:
            raise ValueError("Missing required environment variables")

    def fetch_all_data(self):
        """Run the complete data fetch process"""
        try:
            # Fetch and export campaign data
            print("\nFetching campaign data...")
            campaigns = self.campaign_fetcher.get_campaign_performance()
            self.campaign_fetcher.export_campaign_data(campaigns, str(self.campaign_dir))
            
            time.sleep(30)  # Increased delay between different data types
            
            print("\nFetching ad set data...")
            adsets = self.adset_fetcher.get_adset_performance()
            self.adset_fetcher.export_adset_data(adsets, str(self.adset_dir))
            
            time.sleep(30)
            
            print("\nFetching ad data...")
            ads = self.ad_fetcher.get_ad_performance()
            self.ad_fetcher.export_ad_data(ads, str(self.ad_dir))
            
            print("\nData fetch completed successfully!")
            
        except Exception as e:
            print(f"\nError during execution: {str(e)}")
            raise

def main():
    try:
        fetcher = MetaAdsDataManager()
        fetcher.fetch_all_data()
    except Exception as e:
        print(f"\nScript failed: {str(e)}")

if __name__ == "__main__":
    main()