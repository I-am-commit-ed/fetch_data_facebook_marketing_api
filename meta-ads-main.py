import os
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from utils.api_client import MetaAdsAPIClient
from fetchers.campaign_fetcher import CampaignFetcher
from fetchers.adset_fetcher import AdSetFetcher
from fetchers.ad_fetcher import AdFetcher

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
        """Setup environment variables and paths"""
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

    def fetch_all_data(
        self,
        date_ranges=["7_days", "28_days", "lifetime"],
        attribution_windows=["1d_click", "7d_click", "default"]
    ):
        """Fetch all data from Meta Ads"""
        try:
            print("\nFetching campaign data...")
            campaign_data = self.campaign_fetcher.get_campaign_performance(
                date_ranges,
                attribution_windows
            )
            self.campaign_fetcher.export_campaign_data(campaign_data, self.campaign_dir)

            print("\nFetching ad set data...")
            adset_data = self.adset_fetcher.get_adset_performance(
                date_ranges,
                attribution_windows
            )
            self.adset_fetcher.export_adset_data(adset_data, self.adset_dir)

            print("\nFetching ad data...")
            ad_data = self.ad_fetcher.get_ad_performance(
                date_ranges,
                attribution_windows
            )
            self.ad_fetcher.export_ad_data(ad_data, self.ad_dir)

            print("\nData fetch completed successfully!")
            self._generate_fetch_report()

        except Exception as e:
            print(f"\nError during data fetch: {str(e)}")
            raise

    def _generate_fetch_report(self):
        """Generate a summary report of the data fetch"""
        report = {
            'fetch_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'files_generated': []
        }

        # Count files in each directory
        for directory in [self.campaign_dir, self.adset_dir, self.ad_dir]:
            files = list(directory.glob('*.csv'))
            report['files_generated'].extend([str(f) for f in files])

        # Save report
        report_path = self.output_dir / 'fetch_report.txt'
        with open(report_path, 'w') as f:
            f.write(f"Meta Ads Data Fetch Report\n")
            f.write(f"Generated at: {report['fetch_time']}\n\n")
            f.write(f"Files generated ({len(report['files_generated'])}):\n")
            for file in report['files_generated']:
                f.write(f"- {file}\n")

def main():
    try:
        print("Initializing Meta Ads Data Manager...")
        manager = MetaAdsDataManager()
        
        print("Starting data fetch...")
        manager.fetch_all_data()
        
    except Exception as e:
        print(f"\nScript failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
