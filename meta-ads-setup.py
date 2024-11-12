import os
from pathlib import Path

def create_project_structure():
    """Create the project directory structure and files"""
    # Define the base directory
    base_dir = Path(__file__).parent

    # Define the directory structure
    directories = [
        'meta_ads',
        'meta_ads/utils',
        'meta_ads/fetchers',
        'meta_ads/exports',
        'meta_ads/exports/campaigns',
        'meta_ads/exports/adsets',
        'meta_ads/exports/ads'
    ]

    # Create directories
    for directory in directories:
        dir_path = base_dir / directory
        dir_path.mkdir(parents=True, exist_ok=True)
        print(f"Created directory: {dir_path}")

    # Create __init__.py files
    init_locations = [
        'meta_ads',
        'meta_ads/utils',
        'meta_ads/fetchers'
    ]

    for location in init_locations:
        init_file = base_dir / location / '__init__.py'
        init_file.touch()
        print(f"Created file: {init_file}")

    # Create .env template
    env_template = """
FACEBOOK_ACCESS_TOKEN=your_access_token_here
FACEBOOK_AD_ACCOUNT_ID=your_ad_account_id_here
"""
    
    env_file = base_dir / '.env.template'
    with open(env_file, 'w') as f:
        f.write(env_template.strip())
    print(f"Created file: {env_file}")

    print("\nProject structure created successfully!")
    print("\nNext steps:")
    print("1. Copy .env.template to .env")
    print("2. Update .env with your Meta Ads credentials")
    print("3. Install required dependencies")
    print("4. Run main.py to fetch data")

if __name__ == "__main__":
    create_project_structure()
