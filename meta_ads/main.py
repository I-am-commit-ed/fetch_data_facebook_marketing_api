import os
from pathlib import Path
from dotenv import load_dotenv
from meta_ads.utils.api_client import MetaAdsAPIClient
from meta_ads.fetchers.campaign_fetcher import CampaignFetcher
from meta_ads.fetchers.adset_fetcher import AdSetFetcher
from meta_ads.fetchers.ad_fetcher import AdFetcher

class MetaAdsDataManager:
    def __init__(self):
        self.setup_environment()
        self.api_client = MetaAdsAPIClient(self.access_token, self.account_id)
        self.campaign_fetcher = CampaignFetcher(self.api_client)
        self.adset_fetcher = AdSetFetcher(self.api_client)
        self.ad_fetcher = AdFetcher(self.api_client)
        
        # Create output directories
        self.create_output_directories()

    def setup_environment(self):
        """Setup environment variables"""
        load_dotenv()
        
        self.access_token = os.getenv('FACEBOOK_ACCESS_TOKEN')
        self.account_id = os.getenv('FACEBOOK_AD_ACCOUNT_ID')
        
        if not self.access_token or not self.account_id:
            raise ValueError("Missing required environment variables")
        
        # Setup directories
        self.base_dir = Path(__file__).parent
        self.output_dir = self.base_dir / 'exports'
        self.campaign_dir = self.output_dir / 'campaigns'
        self.adset_dir = self.output_dir / 'adsets'
        self.ad_dir = self.output_dir / 'ads'

    def create_output_directories(self):
        """Create necessary output directories"""
        directories = [self.output_dir, self.campaign_dir, self.adset_dir, self.ad_dir]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)

    def fetch_all_data(self):
        """Run the complete data fetch process"""
        try:
            # Fetch and save campaign data
            print("\nFetching campaign data...")
            campaigns = self.campaign_fetcher.get_campaign_performance()
            
            # Fetch and save ad set data
            print("\nFetching ad set data...")
            adsets = self.adset_fetcher.get_adset_performance()
            
            # Fetch and save ad data
            print("\nFetching ad data...")
            ads = self.ad_fetcher.get_ad_performance()
            
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