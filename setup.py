from setuptools import setup, find_packages

setup(
    name="meta_ads",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        'requests>=2.28.0',
        'pandas>=1.5.0',
        'python-dotenv>=1.0.0',
        'pathlib>=1.0.1',
    ],
    author="Manuel",
    description="Meta Ads Data Fetcher",
    python_requires=">=3.7",
)