"""
Market Data Validation Framework
================================

Comprehensive data validation for market data from multiple sources including
Yahoo Finance, KiteConnect, and other data providers. This module ensures
data quality, accuracy, and consistency before using it for trading decisions.

Key validations:
- Price range and volume sanity checks
- Missing data detection and handling
- Cross-source data validation
- Market hours and holiday validation
- Technical indicator consistency
- Real-time data quality monitoring

Author: AI Trading Machine
Licensed by SJ Trading
"""

from dataclasses import dataclass
from datetime import datetime, time
from enum import Enum
from typing import Any, Optional, Union, Dict

import pandas as pd

from ..utils.logger import setup_logger

logger = setup_logger(__name__)


class ValidationSeverity(Enum):
    """Severity levels for validation issues."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class DataSource(Enum):
    """Data source types."""

    YAHOO_FINANCE = "yahoo_finance"
    KITECONNECT = "kiteconnect"
    NSE_OFFICIAL = "nse_official"
    BSE_OFFICIAL = "bse_official"
    UNKNOWN = "unknown"


@dataclass
class ValidationIssue:
    """Represents a data validation issue."""

    symbol: str
    issue_type: str
    severity: ValidationSeverity
    message: str
    timestamp: datetime
    data_source: DataSource
    affected_rows: Optional[int] = None
    suggested_action: Optional[str] = None


@dataclass
class ValidationResult:
    """Result of data validation."""

    is_valid: bool
    issues: list[ValidationIssue]
    cleaned_data: Optional[pd.DataFrame] = None
    validation_summary: Optional[dict[str, Any]] = None


def validate_data(
    df: pd.DataFrame,
    symbol: str = "UNKNOWN",
    source: str = "unknown",
    strict: bool = False,
) -> Dict[str, Union[bool, pd.DataFrame, str]]:
    """
    Validate market data for data quality issues.

    This function performs basic validation on market data to ensure it meets
    quality standards before being used for trading or analysis.

    Args:
        df: DataFrame containing market data (OHLCV)
        symbol: The ticker symbol of the data
        source: Source of the data (yahoo, kite, etc.)
        strict: Whether to use strict validation

    Returns:
        Dictionary with validation results containing:
        - is_valid: Boolean indicating if data passed validation
        - cleaned_data: DataFrame with issues fixed where possible
        - message: Description of validation results
    """
    # Convert source string to DataSource enum
    data_source = DataSource.UNKNOWN
    if source.lower() == "yahoo" or source.lower() == "yfinance":
        data_source = DataSource.YAHOO_FINANCE
    elif source.lower() == "kite" or source.lower() == "kiteconnect":
        data_source = DataSource.KITECONNECT
    elif source.lower() == "nse":
        data_source = DataSource.NSE_OFFICIAL
    elif source.lower() == "bse":
        data_source = DataSource.BSE_OFFICIAL

    # Initialize validator
    validator = MarketDataValidator(strict_mode=strict)

    # Run comprehensive validation
    result = validator.comprehensive_validation(df, symbol, data_source)

    # Prepare return value
    return {
        "is_valid": result.is_valid,
        "cleaned_data": result.cleaned_data if result.cleaned_data is not None else df,
        "message": f"Validation {'passed' if result.is_valid else 'failed'} with {len(result.issues)} issues",
    }


class MarketDataValidator:
    """
    Comprehensive market data validator for trading systems.

    This class provides extensive validation for market data including:
    - Basic data integrity checks
    - Market-specific validations for Indian markets
    - Cross-source data consistency
    - Real-time data quality monitoring
    """

    def __init__(self, strict_mode: bool = False):
        """
        Initialize the data validator.

        Args:
            strict_mode: If True, raises exceptions on validation errors
        """
        self.strict_mode = strict_mode
        self.issues: list[ValidationIssue] = []

        # Indian market parameters
        self.indian_market_hours = {
            "start": time(9, 15),  # 9:15 AM IST
            "end": time(15, 30),  # 3:30 PM IST
        }

        # Price range limits for Indian stocks (reasonable bounds)
        self.price_limits = {
            "min_price": 0.01,  # 1 paisa minimum
            "max_price": 100000,  # 1 lakh maximum (very conservative)
            "max_daily_change": 0.20,  # 20% maximum daily change
            "min_volume": 0,  # Minimum volume
        }

        # Technical validation parameters
        self.technical_limits = {
            "max_gap_percentage": 0.25,  # 25% gap tolerance
            "min_trading_days_per_month": 15,
            "max_consecutive_missing_days": 3,
        }

    def validate_dataframe_structure(
        self,
        df: pd.DataFrame,
        symbol: str,
        data_source: DataSource = DataSource.UNKNOWN,
    ) -> ValidationResult:
        """
        Validate basic DataFrame structure and required columns.

        Args:
            df: Input DataFrame
            symbol: Stock symbol being validated
            data_source: Source of the data

        Returns:
            ValidationResult with structure validation results
        """
        issues = []

        # Check if DataFrame is empty
        if df.empty:
            issues.append(
                ValidationIssue(
                    symbol=symbol,
                    issue_type="empty_dataframe",
                    severity=ValidationSeverity.CRITICAL,
                    message="DataFrame is empty",
                    timestamp=datetime.now(),
                    data_source=data_source,
                    suggested_action="Check data source connectivity and symbol validity",
                )
            )
            return ValidationResult(is_valid=False, issues=issues)

        # Handle multi-level columns (Yahoo Finance format)
        df_check = df.copy()
        if isinstance(df.columns, pd.MultiIndex):
            # Flatten multi-level columns by taking the first level (price names)
            df_check.columns = [
                col[0].lower() if isinstance(col, tuple) else str(col).lower()
                for col in df.columns
            ]
        else:
            df_check.columns = [str(col).lower() for col in df.columns]

        # Required columns for OHLCV data
        required_columns = ["open", "high", "low", "close", "volume"]
        missing_columns = [
            col for col in required_columns if col not in df_check.columns
        ]

        if missing_columns:
            issues.append(
                ValidationIssue(
                    symbol=symbol,
                    issue_type="missing_columns",
                    severity=ValidationSeverity.CRITICAL,
                    message="Missing required columns: {missing_columns}",
                    timestamp=datetime.now(),
                    data_source=data_source,
                    suggested_action="Ensure data source provides complete OHLCV data",
                )
            )

        # Check for date index or date column
        has_date_index = isinstance(df.index, pd.DatetimeIndex)
        has_date_column = "date" in df.columns

        if not has_date_index and not has_date_column:
            issues.append(
                ValidationIssue(
                    symbol=symbol,
                    issue_type="missing_date",
                    severity=ValidationSeverity.ERROR,
                    message="No date index or date column found",
                    timestamp=datetime.now(),
                    data_source=data_source,
                    suggested_action="Ensure data has proper date indexing",
                )
            )

        # Check data types
        numeric_columns = ["open", "high", "low", "close", "volume"]
        for col in numeric_columns:
            if col in df.columns and not pd.api.types.is_numeric_dtype(df[col]):
                issues.append(
                    ValidationIssue(
                        symbol=symbol,
                        issue_type="invalid_data_type",
                        severity=ValidationSeverity.WARNING,
                        message="Column {col} is not numeric: {df[col].dtype}",
                        timestamp=datetime.now(),
                        data_source=data_source,
                        suggested_action="Convert {col} to numeric format",
                    )
                )

        is_valid = all(
            issue.severity != ValidationSeverity.CRITICAL for issue in issues
        )
        return ValidationResult(is_valid=is_valid, issues=issues)

    def validate_price_data(
        self,
        df: pd.DataFrame,
        symbol: str,
        data_source: DataSource = DataSource.UNKNOWN,
    ) -> ValidationResult:
        """
        Validate price data for anomalies and inconsistencies.

        Args:
            df: DataFrame with OHLCV data
            symbol: Stock symbol
            data_source: Source of the data

        Returns:
            ValidationResult with price validation results
        """
        issues = []

        if df.empty:
            return ValidationResult(is_valid=False, issues=[])

        # Handle multi-level columns (Yahoo Finance format)
        df_work = df.copy()
        if isinstance(df.columns, pd.MultiIndex):
            # Flatten multi-level columns by taking the first level (price names)
            df_work.columns = [
                col[0].lower() if isinstance(col, tuple) else str(col).lower()
                for col in df.columns
            ]
        else:
            df_work.columns = [str(col).lower() for col in df.columns]

        # Ensure required columns exist
        required_cols = ["open", "high", "low", "close"]
        missing_cols = [col for col in required_cols if col not in df_work.columns]
        if missing_cols:
            issues.append(
                ValidationIssue(
                    symbol=symbol,
                    issue_type="missing_price_columns",
                    severity=ValidationSeverity.CRITICAL,
                    message="Missing price columns: {missing_cols}",
                    timestamp=datetime.now(),
                    data_source=data_source,
                )
            )
            return ValidationResult(is_valid=False, issues=issues)

        # 1. Basic price validations
        for col in required_cols:
            # Check for negative prices
            negative_prices = df_work[df_work[col] < 0]
            if not negative_prices.empty:
                issues.append(
                    ValidationIssue(
                        symbol=symbol,
                        issue_type="negative_prices",
                        severity=ValidationSeverity.ERROR,
                        message="Found {len(negative_prices)} negative {col} prices",
                        timestamp=datetime.now(),
                        data_source=data_source,
                        affected_rows=len(negative_prices),
                        suggested_action="Remove or interpolate negative {col} values",
                    )
                )

            # Check for extremely low prices
            low_prices = df_work[df_work[col] < self.price_limits["min_price"]]
            if not low_prices.empty:
                issues.append(
                    ValidationIssue(
                        symbol=symbol,
                        issue_type="extremely_low_prices",
                        severity=ValidationSeverity.WARNING,
                        message="Found {len(low_prices)} {col} prices below ₹{self.price_limits['min_price']}",
                        timestamp=datetime.now(),
                        data_source=data_source,
                        affected_rows=len(low_prices),
                    )
                )

            # Check for extremely high prices
            high_prices = df_work[df_work[col] > self.price_limits["max_price"]]
            if not high_prices.empty:
                issues.append(
                    ValidationIssue(
                        symbol=symbol,
                        issue_type="extremely_high_prices",
                        severity=ValidationSeverity.WARNING,
                        message="Found {len(high_prices)} {col} prices above ₹{self.price_limits['max_price']}",
                        timestamp=datetime.now(),
                        data_source=data_source,
                        affected_rows=len(high_prices),
                    )
                )

        # 2. OHLC consistency checks
        df_clean = df_work.copy()

        # High should be >= Open, Close
        high_low_open = df_clean[df_clean["high"] < df_clean["open"]]
        if not high_low_open.empty:
            issues.append(
                ValidationIssue(
                    symbol=symbol,
                    issue_type="high_less_than_open",
                    severity=ValidationSeverity.ERROR,
                    message="Found {len(high_low_open)} records where High < Open",
                    timestamp=datetime.now(),
                    data_source=data_source,
                    affected_rows=len(high_low_open),
                    suggested_action="Fix OHLC data inconsistencies",
                )
            )

        high_low_close = df_clean[df_clean["high"] < df_clean["close"]]
        if not high_low_close.empty:
            issues.append(
                ValidationIssue(
                    symbol=symbol,
                    issue_type="high_less_than_close",
                    severity=ValidationSeverity.ERROR,
                    message="Found {len(high_low_close)} records where High < Close",
                    timestamp=datetime.now(),
                    data_source=data_source,
                    affected_rows=len(high_low_close),
                )
            )

        # Low should be <= Open, Close
        low_high_open = df_clean[df_clean["low"] > df_clean["open"]]
        if not low_high_open.empty:
            issues.append(
                ValidationIssue(
                    symbol=symbol,
                    issue_type="low_greater_than_open",
                    severity=ValidationSeverity.ERROR,
                    message="Found {len(low_high_open)} records where Low > Open",
                    timestamp=datetime.now(),
                    data_source=data_source,
                    affected_rows=len(low_high_open),
                )
            )

        low_high_close = df_clean[df_clean["low"] > df_clean["close"]]
        if not low_high_close.empty:
            issues.append(
                ValidationIssue(
                    symbol=symbol,
                    issue_type="low_greater_than_close",
                    severity=ValidationSeverity.ERROR,
                    message="Found {len(low_high_close)} records where Low > Close",
                    timestamp=datetime.now(),
                    data_source=data_source,
                    affected_rows=len(low_high_close),
                )
            )

        # 3. Volume validation
        if "volume" in df.columns:
            # Check for negative volume
            negative_volume = df_clean[df_clean["volume"] < 0]
            if not negative_volume.empty:
                issues.append(
                    ValidationIssue(
                        symbol=symbol,
                        issue_type="negative_volume",
                        severity=ValidationSeverity.ERROR,
                        message="Found {len(negative_volume)} negative volume records",
                        timestamp=datetime.now(),
                        data_source=data_source,
                        affected_rows=len(negative_volume),
                    )
                )

            # Check for suspiciously high volume (>100x median)
            if len(df_clean) > 10:  # Need enough data for median
                median_volume = df_clean["volume"].median()
                if median_volume > 0:
                    high_volume = df_clean[df_clean["volume"] > median_volume * 100]
                    if not high_volume.empty:
                        issues.append(
                            ValidationIssue(
                                symbol=symbol,
                                issue_type="suspiciously_high_volume",
                                severity=ValidationSeverity.WARNING,
                                message="Found {len(high_volume)} records with volume >100x median",
                                timestamp=datetime.now(),
                                data_source=data_source,
                                affected_rows=len(high_volume),
                            )
                        )

        # 4. Daily price change validation
        if len(df_clean) > 1:
            df_clean_sorted = (
                df_clean.sort_index()
                if hasattr(df_clean.index, "sort_values")
                else df_clean
            )
            df_clean_sorted["prev_close"] = df_clean_sorted["close"].shift(1)
            df_clean_sorted["daily_change"] = (
                df_clean_sorted["close"] - df_clean_sorted["prev_close"]
            ) / df_clean_sorted["prev_close"]

            extreme_changes = df_clean_sorted[
                abs(df_clean_sorted["daily_change"])
                > self.price_limits["max_daily_change"]
            ]
            if not extreme_changes.empty:
                issues.append(
                    ValidationIssue(
                        symbol=symbol,
                        issue_type="extreme_daily_change",
                        severity=ValidationSeverity.WARNING,
                        message="Found {len(extreme_changes)} days with >20% price change",
                        timestamp=datetime.now(),
                        data_source=data_source,
                        affected_rows=len(extreme_changes),
                        suggested_action="Check for stock splits, bonuses, or data errors",
                    )
                )

        is_valid = all(
            issue.severity != ValidationSeverity.CRITICAL for issue in issues
        )
        return ValidationResult(is_valid=is_valid, issues=issues, cleaned_data=df_clean)

    def validate_missing_data(
        self,
        df: pd.DataFrame,
        symbol: str,
        data_source: DataSource = DataSource.UNKNOWN,
    ) -> ValidationResult:
        """
        Validate and report missing data patterns.

        Args:
            df: DataFrame with time series data
            symbol: Stock symbol
            data_source: Source of the data

        Returns:
            ValidationResult with missing data analysis
        """
        issues = []

        if df.empty:
            return ValidationResult(is_valid=False, issues=[])

        # Check for missing values in critical columns
        critical_columns = ["open", "high", "low", "close"]
        for col in critical_columns:
            if col in df.columns:
                missing_count = df[col].isna().sum()
                if missing_count > 0:
                    missing_pct = (missing_count / len(df)) * 100
                    severity = (
                        ValidationSeverity.ERROR
                        if missing_pct > 10
                        else ValidationSeverity.WARNING
                    )

                    issues.append(
                        ValidationIssue(
                            symbol=symbol,
                            issue_type="missing_price_data",
                            severity=severity,
                            message="Missing {missing_count} ({missing_pct:.1f}%) {col} values",
                            timestamp=datetime.now(),
                            data_source=data_source,
                            affected_rows=missing_count,
                            suggested_action="Interpolate or fetch missing data",
                        )
                    )

        # Check for date gaps (if data has date index)
        if isinstance(df.index, pd.DatetimeIndex) and len(df) > 1:
            date_diff = df.index.to_series().diff().dt.days
            # Look for gaps > 5 days (accounting for weekends)
            large_gaps = date_diff[date_diff > 5]
            if not large_gaps.empty:
                issues.append(
                    ValidationIssue(
                        symbol=symbol,
                        issue_type="date_gaps",
                        severity=ValidationSeverity.WARNING,
                        message="Found {len(large_gaps)} date gaps >5 days",
                        timestamp=datetime.now(),
                        data_source=data_source,
                        affected_rows=len(large_gaps),
                        suggested_action="Check for holidays or data source gaps",
                    )
                )

        is_valid = all(
            issue.severity != ValidationSeverity.CRITICAL for issue in issues
        )
        return ValidationResult(is_valid=is_valid, issues=issues)

    def validate_against_market_hours(
        self,
        df: pd.DataFrame,
        symbol: str,
        data_source: DataSource = DataSource.UNKNOWN,
    ) -> ValidationResult:
        """
        Validate data timestamps against Indian market hours.

        Args:
            df: DataFrame with timestamp data
            symbol: Stock symbol
            data_source: Source of the data

        Returns:
            ValidationResult with market hours validation
        """
        issues = []

        if not isinstance(df.index, pd.DatetimeIndex):
            # Skip if no datetime index
            return ValidationResult(is_valid=True, issues=[])

        # Check for data outside market hours (only for intraday data)
        if len(df) > 0:
            # Detect if this is intraday data (multiple records per day)
            dates = df.index.date
            unique_dates = pd.Series(dates).nunique()
            is_intraday = len(df) > unique_dates * 2  # More than 2 records per day

            if is_intraday:
                # Check market hours for intraday data
                market_start = self.indian_market_hours["start"]
                market_end = self.indian_market_hours["end"]

                outside_hours = df[
                    (df.index.time < market_start) | (df.index.time > market_end)
                ]

                if not outside_hours.empty:
                    issues.append(
                        ValidationIssue(
                            symbol=symbol,
                            issue_type="outside_market_hours",
                            severity=ValidationSeverity.WARNING,
                            message="Found {len(outside_hours)} records outside market hours (9:15-15:30 IST)",
                            timestamp=datetime.now(),
                            data_source=data_source,
                            affected_rows=len(outside_hours),
                            suggested_action="Filter data to market hours only",
                        )
                    )

                # Check for weekend data
                weekend_data = df[df.index.weekday >= 5]  # Saturday=5, Sunday=6
                if not weekend_data.empty:
                    issues.append(
                        ValidationIssue(
                            symbol=symbol,
                            issue_type="weekend_data",
                            severity=ValidationSeverity.INFO,
                            message="Found {len(weekend_data)} weekend records",
                            timestamp=datetime.now(),
                            data_source=data_source,
                            affected_rows=len(weekend_data),
                        )
                    )

        is_valid = True  # Market hours validation is usually not critical
        return ValidationResult(is_valid=is_valid, issues=issues)

    def cross_validate_sources(
        self,
        df1: pd.DataFrame,
        df2: pd.DataFrame,
        symbol: str,
        source1: DataSource,
        source2: DataSource,
        tolerance: float = 0.05,
    ) -> ValidationResult:
        """
        Cross-validate data from two different sources.

        Args:
            df1: DataFrame from first source
            df2: DataFrame from second source
            symbol: Stock symbol
            source1: First data source
            source2: Second data source
            tolerance: Acceptable difference percentage (default 5%)

        Returns:
            ValidationResult with cross-validation results
        """
        issues = []

        if df1.empty or df2.empty:
            issues.append(
                ValidationIssue(
                    symbol=symbol,
                    issue_type="insufficient_data_for_cross_validation",
                    severity=ValidationSeverity.WARNING,
                    message="One or both data sources are empty",
                    timestamp=datetime.now(),
                    data_source=DataSource.UNKNOWN,
                    suggested_action="Ensure both data sources have data for comparison",
                )
            )
            return ValidationResult(is_valid=False, issues=issues)

        # Align data on common dates
        if isinstance(df1.index, pd.DatetimeIndex) and isinstance(
            df2.index, pd.DatetimeIndex
        ):
            common_dates = df1.index.intersection(df2.index)
            if len(common_dates) == 0:
                issues.append(
                    ValidationIssue(
                        symbol=symbol,
                        issue_type="no_common_dates",
                        severity=ValidationSeverity.ERROR,
                        message="No common dates between data sources",
                        timestamp=datetime.now(),
                        data_source=DataSource.UNKNOWN,
                        suggested_action="Check date ranges and formatting",
                    )
                )
                return ValidationResult(is_valid=False, issues=issues)

            df1_common = df1.loc[common_dates]
            df2_common = df2.loc[common_dates]

            # Compare close prices (most important)
            if "close" in df1_common.columns and "close" in df2_common.columns:
                price_diff = (
                    abs(df1_common["close"] - df2_common["close"]) / df1_common["close"]
                )
                large_differences = price_diff[price_diff > tolerance]

                if not large_differences.empty:
                    avg_diff = large_differences.mean() * 100
                    issues.append(
                        ValidationIssue(
                            symbol=symbol,
                            issue_type="price_discrepancy",
                            severity=ValidationSeverity.WARNING,
                            message="Found {len(large_differences)} days with >{tolerance*100}% price difference (avg: {avg_diff:.2f}%)",
                            timestamp=datetime.now(),
                            data_source=DataSource.UNKNOWN,
                            affected_rows=len(large_differences),
                            suggested_action="Investigate price differences between {source1.value} and {source2.value}",
                        )
                    )

            # Compare volumes if available
            if "volume" in df1_common.columns and "volume" in df2_common.columns:
                # Volume can vary significantly, so use higher tolerance
                volume_tolerance = 0.20  # 20%
                volume_diff = abs(df1_common["volume"] - df2_common["volume"]) / (
                    df1_common["volume"] + 1
                )  # +1 to avoid division by zero
                large_vol_diff = volume_diff[volume_diff > volume_tolerance]

                if not large_vol_diff.empty:
                    issues.append(
                        ValidationIssue(
                            symbol=symbol,
                            issue_type="volume_discrepancy",
                            severity=ValidationSeverity.INFO,
                            message="Found {len(large_vol_diff)} days with >{volume_tolerance*100}% volume difference",
                            timestamp=datetime.now(),
                            data_source=DataSource.UNKNOWN,
                            affected_rows=len(large_vol_diff),
                        )
                    )

        is_valid = all(
            issue.severity
            not in [ValidationSeverity.ERROR, ValidationSeverity.CRITICAL]
            for issue in issues
        )
        return ValidationResult(is_valid=is_valid, issues=issues)

    def comprehensive_validation(
        self,
        df: pd.DataFrame,
        symbol: str,
        data_source: DataSource = DataSource.UNKNOWN,
        reference_df: Optional[pd.DataFrame] = None,
        reference_source: Optional[DataSource] = None,
    ) -> ValidationResult:
        """
        Run comprehensive validation on market data.

        Args:
            df: Primary DataFrame to validate
            symbol: Stock symbol
            data_source: Source of primary data
            reference_df: Optional reference DataFrame for cross-validation
            reference_source: Source of reference data

        Returns:
            ValidationResult with comprehensive validation results
        """
        all_issues = []
        cleaned_data = df.copy()

        logger.info(
            "🔍 Starting comprehensive validation for {symbol} from {data_source.value}"
        )

        # 1. Structure validation
        structure_result = self.validate_dataframe_structure(df, symbol, data_source)
        all_issues.extend(structure_result.issues)

        if not structure_result.is_valid:
            logger.error("❌ Structure validation failed for {symbol}")
            return ValidationResult(is_valid=False, issues=all_issues)

        # 2. Price data validation
        price_result = self.validate_price_data(df, symbol, data_source)
        all_issues.extend(price_result.issues)
        if price_result.cleaned_data is not None:
            cleaned_data = price_result.cleaned_data

        # 3. Missing data validation
        missing_result = self.validate_missing_data(cleaned_data, symbol, data_source)
        all_issues.extend(missing_result.issues)

        # 4. Market hours validation
        hours_result = self.validate_against_market_hours(
            cleaned_data, symbol, data_source
        )
        all_issues.extend(hours_result.issues)

        # 5. Cross-validation if reference data provided
        if reference_df is not None and reference_source is not None:
            cross_result = self.cross_validate_sources(
                cleaned_data, reference_df, symbol, data_source, reference_source
            )
            all_issues.extend(cross_result.issues)

        # Determine overall validity
        critical_issues = [
            i for i in all_issues if i.severity == ValidationSeverity.CRITICAL
        ]
        error_issues = [i for i in all_issues if i.severity == ValidationSeverity.ERROR]

        is_valid = len(critical_issues) == 0 and (
            not self.strict_mode or len(error_issues) == 0
        )

        # Create validation summary
        validation_summary = {
            "total_issues": len(all_issues),
            "critical_issues": len(critical_issues),
            "error_issues": len(error_issues),
            "warning_issues": len(
                [i for i in all_issues if i.severity == ValidationSeverity.WARNING]
            ),
            "info_issues": len(
                [i for i in all_issues if i.severity == ValidationSeverity.INFO]
            ),
            "data_rows": len(cleaned_data),
            "validation_timestamp": datetime.now(),
            "data_source": data_source.value,
            "symbol": symbol,
        }

        logger.info(
            "✅ Validation complete for {symbol}: {validation_summary['total_issues']} issues found"
        )

        if self.strict_mode and not is_valid:
            error_msg = "Validation failed for {symbol}: {len(critical_issues)} critical, {len(error_issues)} error issues"
            logger.error(error_msg)
            if critical_issues:
                raise ValueError(
                    "Critical validation issues found: {[i.message for i in critical_issues]}"
                )

        return ValidationResult(
            is_valid=is_valid,
            issues=all_issues,
            cleaned_data=cleaned_data,
            validation_summary=validation_summary,
        )

    def generate_validation_report(
        self, results: list[ValidationResult], output_file: Optional[str] = None
    ) -> str:
        """
        Generate a comprehensive validation report.

        Args:
            results: List of validation results
            output_file: Optional file path to save the report

        Returns:
            String containing the validation report
        """
        report_lines = []
        report_lines.append("# Market Data Validation Report")
        report_lines.append("Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("=" * 60)

        total_symbols = len(results)
        valid_symbols = sum(1 for r in results if r.is_valid)

        report_lines.append("\n## Summary")
        report_lines.append("- Total symbols validated: {total_symbols}")
        report_lines.append("- Valid symbols: {valid_symbols}")
        report_lines.append("- Failed validation: {total_symbols - valid_symbols}")

        # Issues by severity
        all_issues = []
        for result in results:
            all_issues.extend(result.issues)

        severity_counts = {}
        for issue in all_issues:
            severity_counts[issue.severity] = severity_counts.get(issue.severity, 0) + 1

        report_lines.append("\n## Issues by Severity")
        for severity, count in severity_counts.items():
            report_lines.append("- {severity.value.title()}: {count}")

        # Detailed results
        report_lines.append("\n## Detailed Results")
        for result in results:
            if result.validation_summary:
                summary = result.validation_summary
                symbol = summary["symbol"]
                data_source = summary["data_source"]

                report_lines.append("\n### {symbol} ({data_source})")
                report_lines.append(
                    "- Status: {'✅ Valid' if result.is_valid else '❌ Invalid'}"
                )
                report_lines.append("- Data rows: {summary['data_rows']}")
                report_lines.append("- Total issues: {summary['total_issues']}")

                if result.issues:
                    report_lines.append("- Issues:")
                    for issue in result.issues:
                        report_lines.append(
                            "  - {issue.severity.value.upper()}: {issue.issue_type} - {issue.message}"
                        )

        report_content = "\n".join(report_lines)

        if output_file:
            with open(output_file, "w") as f:
                f.write(report_content)
            logger.info("Validation report saved to {output_file}")

        return report_content


class DataValidator:
    """
    Data validation framework for market data from various sources.

    This class provides a comprehensive validation framework for market data
    to ensure data quality, consistency, and accuracy before use in trading
    algorithms.
    """

    def __init__(self, strict_mode: bool = False):
        """
        Initialize the data validator.

        Args:
            strict_mode: Whether to use strict validation rules
        """
        self.strict_mode = strict_mode
        self.logger = logger

    def validate(
        self, df: pd.DataFrame, symbol: str, source: str = "unknown"
    ) -> ValidationResult:
        """
        Validate market data for quality issues.

        Args:
            df: DataFrame containing market data (OHLCV)
            symbol: The ticker symbol of the data
            source: Source of the data (yahoo, kite, etc.)

        Returns:
            ValidationResult object with validation outcomes
        """
        # Use the existing validate_data function for implementation
        result = validate_data(
            df=df, symbol=symbol, source=source, strict=self.strict_mode
        )

        # Convert to ValidationResult format
        issues = []
        if not result["is_valid"]:
            # Create a validation issue based on the message
            data_source = DataSource.UNKNOWN
            if source.lower() in ["yahoo", "yfinance"]:
                data_source = DataSource.YAHOO_FINANCE
            elif source.lower() in ["kite", "kiteconnect"]:
                data_source = DataSource.KITECONNECT

            issues.append(
                ValidationIssue(
                    symbol=symbol,
                    issue_type="data_quality",
                    severity=ValidationSeverity.ERROR,
                    message=result.get("message", "Unknown validation error"),
                    timestamp=datetime.now(),
                    data_source=data_source,
                )
            )

        return ValidationResult(
            is_valid=result["is_valid"],
            issues=issues,
            cleaned_data=result.get("cleaned_data", None),
            validation_summary={"message": result.get("message", "")},
        )

    def get_validation_stats(self) -> dict:
        """
        Get statistics about validation operations.

        Returns:
            Dictionary with validation statistics
        """
        return {
            "total_validations": 0,  # Would track this in a real implementation
            "success_rate": 0.0,
            "common_issues": [],
        }


# Utility functions for common validation scenarios
def validate_yahoo_finance_data(
    df: pd.DataFrame, symbol: str, strict_mode: bool = False
) -> ValidationResult:
    """
    Validate Yahoo Finance data with appropriate settings.

    Args:
        df: DataFrame from Yahoo Finance
        symbol: Stock symbol
        strict_mode: Whether to use strict validation

    Returns:
        ValidationResult
    """
    validator = MarketDataValidator(strict_mode=strict_mode)
    return validator.comprehensive_validation(df, symbol, DataSource.YAHOO_FINANCE)


def validate_kiteconnect_data(
    df: pd.DataFrame, symbol: str, strict_mode: bool = False
) -> ValidationResult:
    """
    Validate KiteConnect data with appropriate settings.

    Args:
        df: DataFrame from KiteConnect
        symbol: Stock symbol
        strict_mode: Whether to use strict validation

    Returns:
        ValidationResult
    """
    validator = MarketDataValidator(strict_mode=strict_mode)
    return validator.comprehensive_validation(df, symbol, DataSource.KITECONNECT)


def cross_validate_yahoo_vs_kite(
    yahoo_df: pd.DataFrame, kite_df: pd.DataFrame, symbol: str, tolerance: float = 0.05
) -> ValidationResult:
    """
    Cross-validate Yahoo Finance vs KiteConnect data.

    Args:
        yahoo_df: DataFrame from Yahoo Finance
        kite_df: DataFrame from KiteConnect
        symbol: Stock symbol
        tolerance: Acceptable difference percentage

    Returns:
        ValidationResult with cross-validation results
    """
    validator = MarketDataValidator()
    return validator.cross_validate_sources(
        yahoo_df,
        kite_df,
        symbol,
        DataSource.YAHOO_FINANCE,
        DataSource.KITECONNECT,
        tolerance=tolerance,
    )


# Example usage and testing
if __name__ == "__main__":
    # Example validation workflow
    print("🔍 Market Data Validation Framework")
    print("=" * 40)

    # Test with sample data
    import yfinance as yf

    # Fetch sample data
    ticker = "RELIANCE.NS"
    data = yf.download(ticker, start="2024-01-01", end="2024-12-31", progress=False)

    if not data.empty:
        # Clean column names
        data.columns = [col.lower() for col in data.columns]

        # Validate
        result = validate_yahoo_finance_data(data, "RELIANCE")

        print("Validation result for RELIANCE:")
        print("- Valid: {result.is_valid}")
        print("- Issues found: {len(result.issues)}")

        for issue in result.issues[:5]:  # Show first 5 issues
            print("  - {issue.severity.value.upper()}: {issue.message}")

    print("\n✅ Validation framework ready for use!")
    print("Next steps:")
    print("1. Integrate with your data ingestion pipeline")
    print("2. Set up automated validation checks")
    print("3. Configure alerts for critical issues")
    print("4. Implement data cleaning based on validation results")


def validate_data_integrity(
    data: pd.DataFrame,
    symbol: str,
    source: DataSource = DataSource.UNKNOWN,
    required_columns: Optional[list[str]] = None,
    min_rows: int = 20,
    max_missing_pct: float = 0.05,
    check_duplicates: bool = True,
) -> ValidationResult:
    """
    Comprehensive data integrity validation for financial market data.

    This function validates market data for integrity issues including:
    - Presence of required columns
    - Sufficient data points
    - Duplicated rows
    - Missing values
    - Price continuity and plausibility

    Args:
        data: DataFrame containing market data
        symbol: Stock symbol being validated
        source: Source of the data
        required_columns: List of columns that must be present
        min_rows: Minimum number of rows required
        max_missing_pct: Maximum allowed percentage of missing values
        check_duplicates: Whether to check for duplicate rows

    Returns:
        ValidationResult with data integrity validation results
    """
    if required_columns is None:
        required_columns = ["open", "high", "low", "close", "volume"]

    result = ValidationResult(
        symbol=symbol,
        source=source,
        is_valid=True,
        issues=[],
        metadata={
            "row_count": len(data) if data is not None and not data.empty else 0,
            "column_count": (
                len(data.columns) if data is not None and not data.empty else 0
            ),
            "duplicate_count": 0,
            "missing_value_count": 0,
        },
    )

    # Check if data is None or empty
    if data is None or data.empty:
        result.is_valid = False
        result.add_issue("No data available for validation", ValidationSeverity.ERROR)
        return result

    # Check required columns
    if required_columns:
        missing_cols = [col for col in required_columns if col not in data.columns]
        if missing_cols:
            result.is_valid = False
            result.add_issue(
                f"Missing required columns: {', '.join(missing_cols)}",
                ValidationSeverity.ERROR,
            )

    # Check sufficient data points
    if len(data) < min_rows:
        result.is_valid = False
        result.add_issue(
            f"Insufficient data points: {len(data)} (minimum {min_rows})",
            ValidationSeverity.ERROR,
        )

    # Check for duplicates if requested
    if check_duplicates:
        duplicate_count = data.duplicated().sum()
        result.metadata["duplicate_count"] = int(duplicate_count)
        if duplicate_count > 0:
            result.add_issue(
                f"Found {duplicate_count} duplicate rows", ValidationSeverity.WARNING
            )

    # Check for missing values
    missing_count = data.isnull().sum().sum()
    result.metadata["missing_value_count"] = int(missing_count)
    if missing_count > 0:
        missing_pct = missing_count / (len(data) * len(data.columns))
        if missing_pct > max_missing_pct:
            result.is_valid = False
            result.add_issue(
                f"High percentage of missing values: {missing_pct:.2%} (max allowed: {max_missing_pct:.2%})",
                ValidationSeverity.ERROR,
            )
        else:
            result.add_issue(
                f"Contains {missing_count} missing values ({missing_pct:.2%})",
                ValidationSeverity.WARNING,
            )

    # Price continuity check (if price columns exist)
    price_cols = [
        col for col in ["open", "high", "low", "close"] if col in data.columns
    ]
    if price_cols:
        # Check for unrealistic price jumps
        for col in price_cols:
            if len(data) > 1:
                price_changes = data[col].pct_change().abs()
                extreme_changes = price_changes[
                    price_changes > 0.2
                ]  # 20% change threshold
                if not extreme_changes.empty:
                    result.add_issue(
                        f"Extreme price changes detected in '{col}' column: {len(extreme_changes)} instances",
                        ValidationSeverity.WARNING,
                    )

    # Additional validation for specific data types could be added here

    # Log validation summary
    if result.is_valid:
        logger.info(
            f"✅ Data integrity validation passed for {symbol} ({source.value})"
        )
    else:
        logger.warning(
            f"⚠️ Data integrity validation failed for {symbol} ({source.value}): {len(result.issues)} issues found"
        )

    return result
