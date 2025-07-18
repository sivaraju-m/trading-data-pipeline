"""
Enhanced Yahoo Finance Data Loader with Comprehensive Validation
================================================================

This module provides robust data loading from Yahoo Finance with built-in
validation, error handling, and data quality checks. It includes:

- Comprehensive data validation using our validation framework
- Error handling and retry logic
- Data cleaning and standardization
- Cross-validation capabilities
- Audit logging for data quality

Author: AI Trading Machine
Licensed by SJ Trading
"""

import time
from datetime import datetime
from typing import Optional

import pandas as pd
import yfinance as yf

from ..utils.logger import setup_logger

# Import our data validation framework
from .data_validator import (
    MarketDataValidator,
    ValidationResult,
    ValidationSeverity,
    validate_yahoo_finance_data,
)

# Initialize logger properly
logger = setup_logger(__name__)

# Initialize MarketDataValidator correctly
market_data_validator = MarketDataValidator(strict_mode=True)

# Update validation_available definition
validation_available = True if "MarketDataValidator" in globals() else False


class EnhancedYFinanceLoader:
    """
    Enhanced Yahoo Finance loader with comprehensive validation and error handling.
    """

    def __init__(self, validate_data: bool = True, strict_mode: bool = False):
        """
        Initialize the enhanced Yahoo Finance loader.

        Args:
            validate_data: Whether to validate downloaded data
            strict_mode: Whether to use strict validation (raises on errors)
        """
        self.validate_data = validate_data and validation_available
        self.strict_mode = strict_mode
        self.validator = (
            MarketDataValidator(strict_mode=strict_mode)
            if validation_available
            else None
        )

        # Download statistics
        self.download_stats = {
            "successful": 0,
            "failed": 0,
            "validation_errors": 0,
            "last_update": None,
        }

        logger.info(
            f"Enhanced YFinance Loader initialized (validation: {self.validate_data})"
        )

    def load_single_symbol(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
        auto_adjust: bool = True,
        retry_count: int = 3,
    ) -> tuple[pd.DataFrame, Optional[ValidationResult]]:
        """
        Load data for a single symbol with validation and error handling.

        Args:
            ticker: Stock ticker symbol
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            auto_adjust: Whether to auto-adjust prices
            retry_count: Number of retry attempts

        Returns:
            Tuple of (DataFrame, ValidationResult)
        """
        validation_result = None

        for attempt in range(retry_count):
            try:
                logger.info(
                    f"📥 Fetching {ticker} (attempt {attempt + 1}/{retry_count})"
                )

                # Download data from Yahoo Finance
                data = yf.download(
                    ticker,
                    start=start_date,
                    end=end_date,
                    progress=False,
                    auto_adjust=auto_adjust,
                )

                # Handle tuple response (yfinance sometimes returns tuple on error)
                if isinstance(data, tuple):
                    data = next(
                        (item for item in data if isinstance(item, pd.DataFrame)), None
                    )
                    if data is None:
                        raise ValueError(
                            f"yfinance returned tuple without DataFrame for {ticker}"
                        )

                # Validate it's a DataFrame
                if not isinstance(data, pd.DataFrame):
                    raise ValueError(
                        f"yfinance returned unexpected type: {type(data)} for {ticker}"
                    )

                if data.empty:
                    logger.warning(
                        f"⚠️ No data returned for {ticker} in range {start_date} to {end_date}"
                    )
                    self.download_stats["failed"] += 1
                    continue

                # Clean and standardize the data
                cleaned_data = self._clean_and_standardize(data, ticker)

                # Validate the data if validation is enabled
                if self.validate_data and self.validator:
                    validation_result = validate_yahoo_finance_data(
                        cleaned_data, ticker, self.strict_mode
                    )

                    if not validation_result.is_valid:
                        critical_issues = [
                            i
                            for i in validation_result.issues
                            if i.severity == ValidationSeverity.CRITICAL
                        ]

                        if critical_issues:
                            logger.error(
                                f"❌ Critical validation issues for {ticker}: "
                                f"{[i.message for i in critical_issues]}"
                            )
                            self.download_stats["validation_errors"] += 1

                            if self.strict_mode:
                                raise ValueError(f"Validation failed for {ticker}")
                            continue
                        else:
                            logger.warning(
                                f"⚠️ Validation warnings for {ticker}: {len(validation_result.issues)} issues"
                            )
                    else:
                        logger.info(f"✅ Data validation passed for {ticker}")

                    # Use cleaned data from validation if available
                    if validation_result.cleaned_data is not None:
                        cleaned_data = validation_result.cleaned_data

                # Success
                self.download_stats["successful"] += 1
                self.download_stats["last_update"] = datetime.now()

                logger.info(
                    f"✅ Successfully loaded {len(cleaned_data)} records for {ticker}"
                )
                return cleaned_data, validation_result

            except Exception as e:
                logger.error(f"❌ Attempt {attempt + 1} failed for {ticker}: {e}")
                if attempt < retry_count - 1:
                    wait_time = 2**attempt  # Exponential backoff
                    logger.info(f"⏳ Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"❌ All attempts failed for {ticker}")
                    self.download_stats["failed"] += 1

        # All attempts failed
        return pd.DataFrame(), validation_result

    def _clean_and_standardize(self, data: pd.DataFrame, ticker: str) -> pd.DataFrame:
        """
        Clean and standardize the DataFrame format.

        Args:
            data: Raw DataFrame from yfinance
            ticker: Ticker symbol for logging

        Returns:
            Cleaned and standardized DataFrame
        """
        df = data.copy()

        # Reset index to get date as column
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()

        # Flatten multiindex columns if present
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [
                "_".join([str(i) for i in col if i and i != ""])
                for col in df.columns.values
            ]

        # Standardize column names: lowercase, remove spaces and ticker suffixes
        original_columns = df.columns.tolist()
        ticker_suffix = f"_{ticker.lower()}"
        df.columns = [
            str(col)
            .lower()
            .replace(" ", "_")
            .replace(ticker_suffix, "")
            .replace(".", "_")
            .strip("_")
            for col in df.columns
        ]

        logger.debug(
            f"[{ticker}] Column mapping: {dict(zip(original_columns, df.columns))}"
        )

        # Convert date column to datetime if needed
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date")

        # Sort by date
        df = df.sort_index()

        # Add symbol column for tracking
        df["symbol"] = ticker

        # Remove any duplicate dates
        duplicate_count = df.index.duplicated().sum()
        if duplicate_count > 0:
            logger.warning(f"⚠️ [{ticker}] Removing {duplicate_count} duplicate dates")
            df = df[~df.index.duplicated(keep="last")]

        return df


# Enhanced legacy function for backward compatibility
def load_yfinance_data_enhanced(
    ticker: str, start_date: str, end_date: str
) -> pd.DataFrame:
    """
    Enhanced data loading with validation for backward compatibility.

    Args:
        ticker: Stock ticker symbol
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        DataFrame with validated price data
    """
    print(
        f"📥 Fetching {ticker} with enhanced validation from {start_date} to {end_date}"
    )

    # Use enhanced loader
    loader = EnhancedYFinanceLoader(validate_data=True, strict_mode=False)
    data, validation = loader.load_single_symbol(ticker, start_date, end_date)

    # Report validation results
    if validation:
        if validation.is_valid:
            print(f"✅ Data validation passed for {ticker}")
        else:
            error_count = len(
                [
                    i
                    for i in validation.issues
                    if i.severity
                    in [ValidationSeverity.CRITICAL, ValidationSeverity.ERROR]
                ]
            )
            warning_count = len(
                [
                    i
                    for i in validation.issues
                    if i.severity == ValidationSeverity.WARNING
                ]
            )
            print(
                f"⚠️ Data validation found {error_count} errors, {warning_count} warnings for {ticker}"
            )

            critical_issues = [
                i
                for i in validation.issues
                if i.severity == ValidationSeverity.CRITICAL
            ]
            if critical_issues:
                print("❌ Critical issues:")
                for issue in critical_issues[:3]:  # Show first 3
                    print(f"   - {issue.message}")

    if data.empty:
        raise ValueError(f"⚠️ No valid data for {ticker} in given range")

    print(f"✅ Loaded {len(data)} validated records for {ticker}")
    return data


# Backward compatibility alias
load_yfinance_data = load_yfinance_data_enhanced
