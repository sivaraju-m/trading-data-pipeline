# Trading Data Pipeline

A modular pipeline for ingesting, validating, and storing financial market data from multiple sources.

## Features

- Multi-source data ingestion (Yahoo Finance, KiteConnect)
- Data validation and cleaning
- BigQuery and GCS integration
- Batch and incremental data updates
- Production-ready monitoring and logging

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/trading-data-pipeline.git
cd trading-data-pipeline

# Install the package
pip install -e .
```

## Usage

```bash
# Run a complete historical data pipeline
pipeline-run --config config/pipeline_config.yaml

# Batch upload data for a specific universe
batch-upload --config config/pipeline_config.yaml --universe nifty50
```

See documentation for more details.
