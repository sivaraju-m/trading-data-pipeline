"""
Data cleaning utilities for financial data.
"""

import numpy as np
import pandas as pd
from typing import Optional


def clean_ohlcv_data(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """
    Clean and validate OHLCV (Open, High, Low, Close, Volume) data.

    This function:
    - Handles missing values
    - Ensures high >= low for each bar
    - Ensures high >= open and high >= close
    - Ensures low <= open and low <= close
    - Fixes negative prices
    - Removes duplicate entries

    Args:
        df: DataFrame containing OHLCV data

    Returns:
        Cleaned DataFrame
    """
    if df is None or df.empty:
        return df

    # Make a copy to avoid modifying the original
    df = df.copy()

    # Sort by date to ensure proper forward filling
    if "date" in df.columns:
        df = df.sort_values(by="date")

    # Forward fill missing values (use previous day's value)
    df.ffill(inplace=True)

    # Backward fill any remaining missing values at the beginning
    df.bfill(inplace=True)

    # Fix negative prices
    for col in ["open", "high", "low", "close"]:
        if col in df.columns:
            mask = df[col] <= 0
            if mask.any():
                # Replace negative values with the average of the previous and next valid values
                df.loc[mask, col] = df[col].abs()

    # Ensure high >= low, high >= open, high >= close
    if all(col in df.columns for col in ["high", "low", "open", "close"]):
        # Fix high < low
        mask = df["high"] < df["low"]
        if mask.any():
            # Swap high and low values
            df.loc[mask, ["high", "low"]] = df.loc[mask, ["low", "high"]].values

        # Fix high < open
        mask = df["high"] < df["open"]
        if mask.any():
            df.loc[mask, "high"] = df.loc[mask, "open"]

        # Fix high < close
        mask = df["high"] < df["close"]
        if mask.any():
            df.loc[mask, "high"] = df.loc[mask, "close"]

        # Fix low > open
        mask = df["low"] > df["open"]
        if mask.any():
            df.loc[mask, "low"] = df.loc[mask, "open"]

        # Fix low > close
        mask = df["low"] > df["close"]
        if mask.any():
            df.loc[mask, "low"] = df.loc[mask, "close"]

    # Fix missing volume
    if "volume" in df.columns:
        mask = (df["volume"].isna()) | (df["volume"] <= 0)
        if mask.any():
            # Replace missing or non-positive volume with median volume
            median_volume = df.loc[~mask, "volume"].median()
            if np.isnan(median_volume):
                median_volume = 1000  # Default value if no valid volumes
            df.loc[mask, "volume"] = median_volume

    # Remove duplicates
    if "date" in df.columns and "symbol" in df.columns:
        df = df.drop_duplicates(subset=["date", "symbol"])
    elif "date" in df.columns:
        df = df.drop_duplicates(subset=["date"])

    return df


def clean_and_impute_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and fill missing data using advanced techniques.

    This is a more comprehensive version of clean_ohlcv_data that may
    include additional cleaning steps for specific use cases.

    Args:
        df: DataFrame to clean

    Returns:
        Cleaned DataFrame
    """
    return clean_ohlcv_data(df)
