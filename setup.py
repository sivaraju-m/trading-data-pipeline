from setuptools import setup, find_packages

setup(
    name="trading-data-pipeline",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pandas>=1.5.0",
        "numpy>=1.22.0",
        "yfinance>=0.2.12",
        "google-cloud-bigquery>=3.3.5",
        "google-cloud-storage>=2.7.0",
        "google-cloud-secret-manager>=2.12.6",
        "kiteconnect>=4.1.0",
        "pyyaml>=6.0",
    ],
    python_requires=">=3.11",
    entry_points={
        "console_scripts": [
            "batch-upload=trading_data_pipeline.bin.batch_upload_historical:main",
            "pipeline-run=trading_data_pipeline.bin.complete_historical_pipeline:main",
        ],
    },
    author="Sivaraju Malladi",
    author_email="sivaraj.malladi@example.com",
    description="Data pipeline for ingesting financial market data",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/sivaraju-m/trading-data-pipeline",
)
