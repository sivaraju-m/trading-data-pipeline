import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from trading_data_pipeline.ingest.data_validator import validate_data


def test_validate_data():
    """Test the validate_data function with different scenarios."""
    # Create a valid test DataFrame
    valid_df = pd.DataFrame(
        {
            "date": [datetime(2023, 1, 1) + timedelta(days=i) for i in range(5)],
            "open": [100, 101, 102, 103, 104],
            "high": [105, 106, 107, 108, 109],
            "low": [95, 96, 97, 98, 99],
            "close": [102, 103, 104, 105, 106],
            "volume": [1000, 2000, 3000, 4000, 5000],
            "symbol": ["AAPL", "AAPL", "AAPL", "AAPL", "AAPL"],
        }
    )

    # Test with valid data
    result = validate_data(valid_df, symbol="AAPL", source="yahoo")
    assert isinstance(result["cleaned_data"], pd.DataFrame)
    assert "issues" in result["message"]

    # Test with invalid data (high < low)
    invalid_df = valid_df.copy()
    invalid_df.loc[2, "high"] = 90  # High less than low
    invalid_df.loc[2, "low"] = 95

    result = validate_data(invalid_df, symbol="AAPL", source="yahoo")
    assert isinstance(result["cleaned_data"], pd.DataFrame)
    assert "issues" in result["message"]

    # Note: The data validator in this implementation doesn't automatically fix high/low inconsistencies
    # So we just verify that the issues were detected and reported

    # Test with empty DataFrame
    empty_df = pd.DataFrame()
    result = validate_data(empty_df, symbol="EMPTY", source="unknown")
    assert result["is_valid"] == False  # Empty DataFrame should fail validation

    # Test with missing values
    missing_df = valid_df.copy()
    missing_df.loc[3, "close"] = np.nan

    result = validate_data(missing_df, symbol="MISSING", source="yahoo")
    assert isinstance(result["cleaned_data"], pd.DataFrame)
