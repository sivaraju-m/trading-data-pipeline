"""
Trading data pipeline ingest module

This module provides data ingestion functionality for the trading system.
"""

from trading_data_pipeline.ingest.tiered_data_fetcher import (
    TieredDataFetcher,
    DataSourceTier,
    FetchStrategy,
    ImputationMethod,
)
from trading_data_pipeline.ingest.data_validator import (
    DataValidator,
    ValidationResult,
    validate_data_integrity,
)
from trading_data_pipeline.ingest.data_cleaner import (
    clean_ohlcv_data,
    handle_negative_prices,
)

# For backward compatibility
# Alias for backward compatibility
data_fetcher = TieredDataFetcher
data_fetcher = TieredDataFetcher

__all__ = [
    "TieredDataFetcher",
    "DataSourceTier",
    "DataValidator",
    "ValidationResult",
    "clean_ohlcv_data",
    "handle_negative_prices",
    "data_fetcher",
]
