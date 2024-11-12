import os
from pathlib import Path
from dotenv import load_dotenv
import requests
import pandas as pd
from datetime import datetime, timedelta
import json
from typing import List, Dict, Any

class MetaAdsDataFetcher:
    def __init__(self):
        self.base_dir = Path('/Users/manuel/Documents/GitHub/JeanPierreWeill/DataExtract')
        self.data_dir = self.base_dir / 'data' / 'meta_ads'
        self.env_path = self.base_dir / '.env'
        
        # Create data directory
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Load environment variables
        if not self.env_path.exists():
            raise FileNotFoundError(f"Environment file not found at {self.env_path}")
        
        load_dotenv(dotenv_path=self.env_path)
        
        self.access_token = os.getenv('FACEBOOK_ACCESS_TOKEN')
        self.ad_account_id = os.getenv('FACEBOOK_AD_ACCOUNT_ID')
        
        if not self.access_token:
            raise ValueError("FACEBOOK_ACCESS_TOKEN not found in environment variables")
        if not self.ad_account_id:
            raise ValueError("FACEBOOK_AD_ACCOUNT_ID not found in environment variables")
            
        self.ad_account_id = self.ad_account_id.replace('act_', '')
        self.base_url = "https://graph.facebook.com/v18.0"
        
        self.validate_credentials()

    def validate_credentials(self):
        """Validate Facebook credentials before proceeding"""
        url = f"{self.base_url}/act_{self.ad_account_id}"
        params = {
            "access_token": self.access_token,
            "fields": "name"
        }
        
        response = requests.get(url=url, params=params)
        if response.status_code != 200:
            raise ValueError(f"Failed to authenticate with Facebook API: {response.text}")
        print(f"Successfully authenticated with ad account: {response.json().get('name', 'Unknown')}")

    def make_api_request(self, endpoint: str, params: Dict[str, Any]) -> List[Dict]:
        """Make API request with error handling"""
        params["access_token"] = self.access_token
        
        try:
            response = requests.get(url=endpoint, params=params)
            response.raise_for_status()
            
            data = response.json().get('data', [])
            paging = response.json().get('paging', {})
            
            # Handle pagination
            while 'next' in paging:
                next_response = requests.get(paging['next'])
                next_response.raise_for_status()
                data.extend(next_response.json().get('data', []))
                paging = next_response.json().get('paging', {})
            
            return data
            
        except requests.exceptions.RequestException as e:
            print(f"API Error: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response: {e.response.text}")
            return []

    def fetch_campaign_data(self) -> List[Dict]:
        """Fetch campaign level data including insights"""
        print("\nFetching campaign data...")
        endpoint = f"{self.base_url}/act_{self.ad_account_id}/campaigns"
        
        # Campaign fields
        fields = [
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
        
        params = {
            "fields": ",".join(fields),
            "limit": 500  # Increase if you have more campaigns
        }
        
        campaigns = self.make_api_request(endpoint, params)
        
        # Fetch insights for all campaigns
        campaign_ids = [c['id'] for c in campaigns]
        insights = self.fetch_campaign_insights(campaign_ids)
        
        # Merge insights with campaign data
        for campaign in campaigns:
            campaign_insights = insights.get(campaign['id'], {})
            campaign.update(campaign_insights)
        
        return campaigns

    def fetch_campaign_insights(self, campaign_ids: List[str]) -> Dict:
        """Fetch insights for campaigns"""
        print("Fetching campaign insights...")
        endpoint = f"{self.base_url}/act_{self.ad_account_id}/insights"
        
        params = {
            "level": "campaign",
            "fields": (
                "campaign_id,campaign_name,"
                "impressions,reach,clicks,"
                "inline_link_clicks,outbound_clicks,"
                "actions,spend,purchase_roas"
            ),
            "action_breakdowns": ["action_type"],
            "time_range": json.dumps({
                "since": "2021-01-01",
                "until": datetime.now().strftime("%Y-%m-%d")
            }),
            "filtering": json.dumps([{
                "field": "campaign.id",
                "operator": "IN",
                "value": campaign_ids
            }])
        }
        
        insights_data = self.make_api_request(endpoint, params)
        
        # Process insights into a dictionary keyed by campaign_id
        insights_dict = {}
        for insight in insights_data:
            campaign_id = insight.get('campaign_id')
            if campaign_id:
                insights_dict[campaign_id] = {
                    'impressions': int(insight.get('impressions', 0)),
                    'reach': int(insight.get('reach', 0)),
                    'clicks': int(insight.get('clicks', 0)),
                    'inline_link_clicks': int(insight.get('inline_link_clicks', 0)),
                    'spend': float(insight.get('spend', 0)),
                    'purchase_roas': float(insight.get('purchase_roas', [{'value': 0}])[0]['value'])
                    if insight.get('purchase_roas') else 0
                }
                
                # Process actions
                actions = insight.get('actions', [])
                for action in actions:
                    action_type = action.get('action_type', '').lower().replace(' ', '_')
                    insights_dict[campaign_id][f'action_{action_type}'] = int(action.get('value', 0))
        
        return insights_dict

    def fetch_adset_data(self) -> List[Dict]:
        """Fetch ad set level data including targeting"""
        print("\nFetching ad set data...")
        endpoint = f"{self.base_url}/act_{self.ad_account_id}/adsets"
        
        fields = [
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
        
        params = {
            "fields": ",".join(fields),
            "limit": 500
        }
        
        adsets = self.make_api_request(endpoint, params)
        
        # Process targeting information
        for adset in adsets:
            targeting = adset.get('targeting', {})
            if targeting:
                adset.update({
                    'countries': targeting.get('geo_locations', {}).get('countries', []),
                    'age_min': targeting.get('age_min'),
                    'age_max': targeting.get('age_max'),
                    'genders': targeting.get('genders', []),
                    'custom_audiences': [
                        audience.get('name') 
                        for audience in targeting.get('custom_audiences', [])
                    ],
                    'excluded_custom_audiences': [
                        audience.get('name') 
                        for audience in targeting.get('excluded_custom_audiences', [])
                    ]
                })
            
        return adsets

    def fetch_ad_data(self) -> List[Dict]:
        """Fetch ad level data including creative information"""
        print("\nFetching ad data...")
        endpoint = f"{self.base_url}/act_{self.ad_account_id}/ads"
        
        fields = [
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
        
        params = {
            "fields": ",".join(fields),
            "limit": 500
        }
        
        ads = self.make_api_request(endpoint, params)
        
        # Fetch creative details for each ad
        for ad in ads:
            creative_id = ad.get('creative', {}).get('id')
            if creative_id:
                creative_details = self.fetch_creative_details(creative_id)
                ad['creative_details'] = creative_details
        
        return ads

    def fetch_creative_details(self, creative_id: str) -> Dict:
        """Fetch detailed creative information"""
        endpoint = f"{self.base_url}/{creative_id}"
        
        fields = [
            "body",
            "call_to_action_type",
            "object_type",
            "image_url",
            "video_id",
            "link_url",
            "platform_customizations",
            "asset_feed_spec"
        ]
        
        params = {
            "fields": ",".join(fields)
        }
        
        creative_data = self.make_api_request(endpoint, params)
        return creative_data[0] if creative_data else {}

    def save_to_csv(self, data: List[Dict], filename: str):
        """Save data to CSV file"""
        if not data:
            print(f"No data to save for {filename}")
            return
            
        df = pd.DataFrame(data)
        file_path = self.data_dir / filename
        df.to_csv(file_path, index=False)
        print(f"Saved {len(df)} records to {file_path}")

    def run(self):
        """Run the complete data fetch process"""
        try:
            # Fetch and save campaign data
            campaigns = self.fetch_campaign_data()
            self.save_to_csv(campaigns, "campaign_data.csv")
            
            # Fetch and save ad set data
            adsets = self.fetch_adset_data()
            self.save_to_csv(adsets, "adset_data.csv")
            
            # Fetch and save ad data
            ads = self.fetch_ad_data()
            self.save_to_csv(ads, "ad_data.csv")
            
            print("\nData fetch completed successfully!")
            
        except Exception as e:
            print(f"\nError during execution: {str(e)}")
            raise

if __name__ == "__main__":
    try:
        fetcher = MetaAdsDataFetcher()
        fetcher.run()
    except Exception as e:
        print(f"\nScript failed: {str(e)}")