"""
Tests for data_cleaner module.
"""

import pandas as pd
import numpy as np
import pytest
from datetime import datetime, timedelta
from trading_data_pipeline.utils.data_cleaner import clean_ohlcv_data


def test_clean_ohlcv_data():
    """Test clean_ohlcv_data function with various test cases."""
    # Create test data
    df = pd.DataFrame(
        {
            "date": [datetime(2023, 1, 1) + timedelta(days=i) for i in range(5)],
            "open": [100, 101, np.nan, 103, 104],
            "high": [105, 106, 107, 108, 99],  # Last high is less than close/open
            "low": [95, 96, 97, 110, 98],  # Fourth low is higher than high
            "close": [102, 103, 104, 105, 106],
            "volume": [1000, 2000, np.nan, 4000, 5000],
            "symbol": ["AAPL", "AAPL", "AAPL", "AAPL", "AAPL"],
        }
    )

    # Clean the data
    cleaned_df = clean_ohlcv_data(df)

    # Assertions
    assert len(cleaned_df) == 5, "Should have same number of rows"

    # Check NaN values were filled
    assert not cleaned_df["open"].isna().any(), "NaN values in 'open' should be filled"
    assert (
        not cleaned_df["volume"].isna().any()
    ), "NaN values in 'volume' should be filled"

    # Check high/low corrections
    assert (
        cleaned_df.loc[3, "high"] >= cleaned_df.loc[3, "low"]
    ), "High should be >= low"
    assert (
        cleaned_df.loc[4, "high"] >= cleaned_df.loc[4, "open"]
    ), "High should be >= open"
    assert (
        cleaned_df.loc[4, "high"] >= cleaned_df.loc[4, "close"]
    ), "High should be >= close"

    # Test empty DataFrame
    empty_df = pd.DataFrame()
    assert clean_ohlcv_data(empty_df).empty, "Empty DataFrame should remain empty"

    # Test None
    assert clean_ohlcv_data(None) is None, "None should return None"


def test_negative_prices():
    """Test handling of negative prices."""
    df = pd.DataFrame(
        {
            "date": [datetime(2023, 1, 1) + timedelta(days=i) for i in range(3)],
            "open": [100, -10, 103],
            "high": [105, 106, 107],
            "low": [95, 96, -5],
            "close": [102, 103, 104],
            "symbol": ["AAPL", "AAPL", "AAPL"],
        }
    )

    cleaned_df = clean_ohlcv_data(df)

    # Negative prices should be replaced
    assert cleaned_df.loc[1, "open"] > 0, "Negative open price should be replaced"
    assert cleaned_df.loc[2, "low"] > 0, "Negative low price should be replaced"
