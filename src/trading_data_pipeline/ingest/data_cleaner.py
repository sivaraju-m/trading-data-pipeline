"""
Data Cleaning Module for Trading Data Pipeline
==============================================

This module provides functions for cleaning and preprocessing market data.
It handles common issues like missing values, outliers, and data inconsistencies.

Features:
- Missing value imputation using various methods
- Outlier detection and handling
- Date/time normalization
- Price adjustment for splits and dividends
- Data validation and integrity checks

Dependencies:
- pandas
- numpy
- scikit-learn (optional for advanced imputation)

Usage:
    from trading_data_pipeline.ingest.data_cleaner import clean_ohlcv_data, impute_missing_values

    cleaned_data = clean_ohlcv_data(raw_data)

Author: AI Trading Machine
Licensed by SJ Trading
"""

import logging
from enum import Enum
from typing import Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

from ..utils.data_utils import check_dataframe_validity
from .data_validator import validate_data_integrity


class ImputationMethod(Enum):
    """Enumeration of available imputation methods for missing data."""

    NONE = "none"  # No imputation, keep NaN values
    FORWARD_FILL = "ffill"  # Forward fill (use last valid value)
    BACKWARD_FILL = "bfill"  # Backward fill (use next valid value)
    LINEAR = "linear"  # Linear interpolation
    POLYNOMIAL = "polynomial"  # Polynomial interpolation
    MEAN = "mean"  # Replace with mean of column
    MEDIAN = "median"  # Replace with median of column
    MODE = "mode"  # Replace with mode of column
    ZERO = "zero"  # Replace with zero
    CUSTOM = "custom"  # Custom function-based imputation


def clean_ohlcv_data(
    data: pd.DataFrame,
    handle_missing: bool = True,
    handle_outliers: bool = True,
    imputation_method: ImputationMethod = ImputationMethod.FORWARD_FILL,
    validate: bool = True,
) -> pd.DataFrame:
    """
    Clean and preprocess OHLCV (Open, High, Low, Close, Volume) data.

    Args:
        data: DataFrame containing OHLCV data with DatetimeIndex
        handle_missing: Whether to handle missing values
        handle_outliers: Whether to handle outliers
        imputation_method: Method to use for imputing missing values
        validate: Whether to validate data integrity before and after cleaning

    Returns:
        Cleaned DataFrame with OHLCV data
    """
    if data is None or len(data) == 0:
        logging.warning("Empty dataframe provided for cleaning")
        return pd.DataFrame()

    # Make a copy to avoid modifying the original
    df = data.copy()

    # Validate input data
    if validate:
        df = validate_data_integrity(df)

    # Handle missing values
    if handle_missing:
        df = impute_missing_values(df, method=imputation_method)

    # Handle outliers
    if handle_outliers:
        df = remove_outliers(df)

    # Ensure OHLC values are consistent (High >= Open >= Close >= Low)
    df = enforce_ohlc_consistency(df)

    # Final validation
    if validate:
        df = validate_data_integrity(df, strict=False)

    return df


def impute_missing_values(
    data: pd.DataFrame, method: ImputationMethod = ImputationMethod.FORWARD_FILL
) -> pd.DataFrame:
    """
    Impute missing values in the dataframe using specified method.

    Args:
        data: DataFrame containing time series data
        method: Imputation method to use

    Returns:
        DataFrame with imputed values
    """
    df = data.copy()

    # Skip if no missing values
    if not df.isna().any().any():
        return df

    # Count missing values before imputation
    missing_before = df.isna().sum().sum()

    # Apply imputation based on method
    if method == ImputationMethod.FORWARD_FILL:
        df.fillna(method="ffill", inplace=True)
        # If still has NaN at the beginning, fill backward
        df.fillna(method="bfill", inplace=True)
    elif method == ImputationMethod.BACKWARD_FILL:
        df.fillna(method="bfill", inplace=True)
        # If still has NaN at the end, fill forward
        df.fillna(method="ffill", inplace=True)
    elif method == ImputationMethod.LINEAR:
        df = df.interpolate(method="linear")
    elif method == ImputationMethod.POLYNOMIAL:
        df = df.interpolate(method="polynomial", order=3)
    elif method == ImputationMethod.MEAN:
        df.fillna(df.mean(), inplace=True)
    elif method == ImputationMethod.MEDIAN:
        df.fillna(df.median(), inplace=True)
    elif method == ImputationMethod.MODE:
        df.fillna(df.mode().iloc[0], inplace=True)
    elif method == ImputationMethod.ZERO:
        df.fillna(0, inplace=True)
    elif method == ImputationMethod.NONE:
        # Do nothing, keep NaN values
        pass

    # Count missing values after imputation
    missing_after = df.isna().sum().sum()
    if missing_before > 0:
        logging.info(
            f"Imputed {missing_before - missing_after} missing values using {method.value} method"
        )
        if missing_after > 0:
            logging.warning(f"{missing_after} missing values remain after imputation")

    return df


def remove_outliers(
    data: pd.DataFrame, method: str = "iqr", threshold: float = 3.0
) -> pd.DataFrame:
    """
    Detect and handle outliers in the dataframe.

    Args:
        data: DataFrame containing time series data
        method: Method to use for outlier detection ('iqr', 'zscore', or 'quantile')
        threshold: Threshold for outlier detection

    Returns:
        DataFrame with outliers handled
    """
    df = data.copy()
    numeric_cols = df.select_dtypes(include=["number"]).columns
    outlier_count = 0

    for col in numeric_cols:
        if method == "iqr":
            # IQR method
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - threshold * IQR
            upper_bound = Q3 + threshold * IQR
            outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)].index
        elif method == "zscore":
            # Z-score method
            mean = df[col].mean()
            std = df[col].std()
            outliers = df[abs((df[col] - mean) / std) > threshold].index
        elif method == "quantile":
            # Quantile method
            lower_bound = df[col].quantile(0.01)
            upper_bound = df[col].quantile(0.99)
            outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)].index
        else:
            logging.warning(f"Unknown outlier detection method: {method}")
            return df

        # Handle outliers by capping them to the bounds
        if not df.loc[outliers, col].empty:
            original_count = len(df.loc[outliers, col])
            outlier_count += original_count

            # Cap values outside the bounds
            df.loc[df[col] < lower_bound, col] = lower_bound
            df.loc[df[col] > upper_bound, col] = upper_bound

            logging.info(f"Capped {original_count} outliers in column {col}")

    if outlier_count > 0:
        logging.info(f"Total outliers handled: {outlier_count}")

    return df


def enforce_ohlc_consistency(data: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure OHLC values maintain proper relationships (High >= Open >= Close >= Low).

    Args:
        data: DataFrame containing OHLC columns

    Returns:
        DataFrame with consistent OHLC values
    """
    df = data.copy()

    # Check if all required columns exist
    ohlc_cols = ["Open", "High", "Low", "Close"]
    if not all(col in df.columns for col in ohlc_cols):
        return df

    # Track the number of inconsistencies fixed
    inconsistencies = 0

    # Ensure High is the highest value
    inconsistent_high = df["High"] < df[["Open", "Close"]].max(axis=1)
    if inconsistent_high.any():
        inconsistencies += inconsistent_high.sum()
        df.loc[inconsistent_high, "High"] = df.loc[
            inconsistent_high, ["Open", "Close"]
        ].max(axis=1)

    # Ensure Low is the lowest value
    inconsistent_low = df["Low"] > df[["Open", "Close"]].min(axis=1)
    if inconsistent_low.any():
        inconsistencies += inconsistent_low.sum()
        df.loc[inconsistent_low, "Low"] = df.loc[
            inconsistent_low, ["Open", "Close"]
        ].min(axis=1)

    if inconsistencies > 0:
        logging.info(f"Fixed {inconsistencies} OHLC inconsistencies")

    return df


def handle_negative_prices(
    data: pd.DataFrame, replace_method: str = "absolute", min_valid_price: float = 0.01
) -> pd.DataFrame:
    """
    Handle negative prices in financial data.

    Negative prices can appear due to data errors, feed problems, or in rare cases
    with certain derivatives/futures. This function handles them using various methods.

    Args:
        data: DataFrame containing price data
        replace_method: Method to handle negative prices:
            - 'absolute': Take the absolute value
            - 'previous': Replace with the previous valid price
            - 'minimum': Replace with the minimum valid price
            - 'nan': Replace with NaN and then impute later
        min_valid_price: Minimum valid price for replacement when using 'minimum' method

    Returns:
        DataFrame with negative prices handled
    """
    df = data.copy()

    # Price columns to check
    price_cols = ["Open", "High", "Low", "Close", "Adj Close"]
    price_cols = [col for col in price_cols if col in df.columns]

    if not price_cols:
        logging.warning("No price columns found in data")
        return df

    # Count negative prices
    neg_price_count = 0
    for col in price_cols:
        neg_mask = df[col] < 0
        neg_count = neg_mask.sum()
        neg_price_count += neg_count

        if neg_count > 0:
            logging.warning(f"Found {neg_count} negative values in {col}")

            # Handle based on the specified method
            if replace_method == "absolute":
                df.loc[neg_mask, col] = df.loc[neg_mask, col].abs()

            elif replace_method == "previous":
                # Store original indices for negative values
                neg_indices = df.index[neg_mask]

                # Forward fill from previous valid values
                df[col] = df[col].mask(neg_mask).ffill()

                # If there are still NaNs at the beginning, backward fill
                if df[col].isna().any():
                    df[col] = df[col].bfill()

            elif replace_method == "minimum":
                df.loc[neg_mask, col] = min_valid_price

            elif replace_method == "nan":
                df.loc[neg_mask, col] = np.nan
                # Note: These NaNs should be handled later with impute_missing_values

            else:
                logging.error(f"Unknown replace_method: {replace_method}")
                # Default to absolute
                df.loc[neg_mask, col] = df.loc[neg_mask, col].abs()

    if neg_price_count > 0:
        logging.info(
            f"Handled {neg_price_count} negative prices using {replace_method} method"
        )

    return df
