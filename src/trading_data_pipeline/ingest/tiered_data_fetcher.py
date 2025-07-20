"""
Tiered Data Fetching System with Validation and Imputation
==========================================================

This module implements a robust multi-source data fetching strategy:
1. Primary: KiteConnect (real-time, official NSE/BSE data)
2. Fallback: Yahoo Finance (free alternative)
3. Validation: Comprehensive quality checks on all data
4. Imputation: Smart algorithms to fill gaps and fix issues

Key Features:
- Automatic failover between data sources
- Real-time data quality monitoring
- Intelligent gap filling using multiple methods
- Cross-source validation and reconciliation
- SEBI-compliant data handling

Author: AI Trading Machine
Licensed by SJ Trading
"""

import warnings
from datetime import datetime
from enum import Enum, auto
from typing import Any, Optional

import pandas as pd

from ..utils.logger import setup_logger
from .data_validator import (
    DataSource,
    MarketDataValidator,
    ValidationResult,
    ValidationSeverity,
    validate_kiteconnect_data,
    validate_yahoo_finance_data,
)


class DataSourceTier(Enum):
    """
    Enum defining the tiers of data sources, ranked by reliability and preference.
    """

    PRIMARY = auto()  # Most reliable (KiteConnect, Bloomberg, etc.)
    SECONDARY = auto()  # Good reliability (Yahoo Finance, AlphaVantage, etc.)
    TERTIARY = auto()  # Less reliable (Free APIs, etc.)
    CACHED = auto()  # Local/cached data
    IMPUTED = auto()  # Data that has been algorithmically generated


# Import data loaders
try:
    from .kite_loader import KiteConnectLoader

    KITE_AVAILABLE = True
except ImportError:
    KITE_AVAILABLE = False
    warnings.warn("KiteConnect not available")

try:
    import yfinance as yf

    YAHOO_AVAILABLE = True
except ImportError:
    YAHOO_AVAILABLE = False
    warnings.warn("Yahoo Finance not available")

logger = setup_logger(__name__)


class DataQuality(Enum):
    """Data quality levels."""

    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    UNUSABLE = "unusable"


class FetchStrategy(Enum):
    """Data fetching strategies."""

    KITE_ONLY = "kite_only"
    YAHOO_ONLY = "yahoo_only"
    KITE_PREFERRED = "kite_preferred"
    BEST_QUALITY = "best_quality"
    REDUNDANT = "redundant"


class ImputationMethod(Enum):
    """Data imputation methods."""

    FORWARD_FILL = "forward_fill"
    BACKWARD_FILL = "backward_fill"
    LINEAR_INTERPOLATION = "linear_interpolation"
    SPLINE_INTERPOLATION = "spline_interpolation"
    CROSS_SOURCE_MERGE = "cross_source_merge"
    MARKET_AVERAGE = "market_average"


class TieredDataFetcher:
    """
    Advanced tiered data fetching system with validation and imputation.

    Fetching Priority:
    1. KiteConnect (primary) - Real-time official data
    2. Yahoo Finance (fallback) - Free alternative
    3. Cross-validation and reconciliation
    4. Intelligent imputation for gaps/issues
    """

    def __init__(
        self,
        kite_api_key: Optional[str] = None,
        kite_access_token: Optional[str] = None,
        strategy: FetchStrategy = FetchStrategy.KITE_PREFERRED,
        validation_strict: bool = False,
        cache_duration: int = 300,  # 5 minutes cache
    ):
        """
        Initialize the tiered data fetcher.

        Args:
            kite_api_key: KiteConnect API key
            kite_access_token: KiteConnect access token
            strategy: Data fetching strategy
            validation_strict: Whether to use strict validation
            cache_duration: Cache duration in seconds
        """
        self.strategy = strategy
        self.validation_strict = validation_strict
        self.cache_duration = cache_duration

        # Initialize data sources
        self.kite_loader = None
        if KITE_AVAILABLE and kite_api_key:
            try:
                self.kite_loader = KiteConnectLoader(
                    api_key=kite_api_key, access_token=kite_access_token
                )
                logger.info("✅ KiteConnect loader initialized")
            except Exception as e:
                logger.warning("⚠️ KiteConnect initialization failed: {e}")

        # Initialize validator
        self.validator = MarketDataValidator(strict_mode=validation_strict)

        # Data cache
        self.cache = {}

        # Statistics tracking
        self.stats = {
            "kite_success": 0,
            "kite_failures": 0,
            "yahoo_success": 0,
            "yahoo_failures": 0,
            "validation_passes": 0,
            "validation_failures": 0,
            "imputation_events": 0,
            "last_reset": datetime.now(),
        }

        logger.info("🔧 Tiered Data Fetcher initialized (strategy: {strategy.value})")

    def fetch_kite_data(
        self, symbol: str, start_date: str, end_date: str, interval: str = "day"
    ) -> tuple[Optional[pd.DataFrame], ValidationResult]:
        """
        Fetch data from KiteConnect with validation.

        Args:
            symbol: Stock symbol (without .NS suffix)
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            interval: Data interval (day, minute, etc.)

        Returns:
            Tuple of (DataFrame or None, ValidationResult)
        """
        if not self.kite_loader:
            logger.warning("❌ KiteConnect not available")
            return None, ValidationResult(is_valid=False, issues=[])

        try:
            logger.info("📥 Fetching {symbol} from KiteConnect...")

            # Fetch data using KiteConnect
            data = self.kite_loader.get_historical_data(
                symbol=symbol, from_date=start_date, to_date=end_date, interval=interval
            )

            if data is None or data.empty:
                logger.warning("⚠️ No data received from KiteConnect for {symbol}")
                self.stats["kite_failures"] += 1
                return None, ValidationResult(is_valid=False, issues=[])

            # Validate KiteConnect data
            validation_result = validate_kiteconnect_data(
                data, symbol, strict_mode=self.validation_strict
            )

            if validation_result.is_valid:
                self.stats["kite_success"] += 1
                self.stats["validation_passes"] += 1
                logger.info("✅ KiteConnect data validated for {symbol}")
            else:
                self.stats["kite_failures"] += 1
                self.stats["validation_failures"] += 1
                logger.warning("❌ KiteConnect data validation failed for {symbol}")

            return data, validation_result

        except Exception as e:
            logger.error("💥 KiteConnect fetch failed for {symbol}: {e}")
            self.stats["kite_failures"] += 1
            return None, ValidationResult(is_valid=False, issues=[])

    def fetch_yahoo_data(
        self, symbol: str, start_date: str, end_date: str
    ) -> tuple[Optional[pd.DataFrame], ValidationResult]:
        """
        Fetch data from Yahoo Finance with validation.

        Args:
            symbol: Stock symbol (will add .NS suffix)
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            Tuple of (DataFrame or None, ValidationResult)
        """
        if not YAHOO_AVAILABLE:
            logger.warning("❌ Yahoo Finance not available")
            return None, ValidationResult(is_valid=False, issues=[])

        try:
            yahoo_symbol = "{symbol}.NS"
            logger.info("📥 Fetching {yahoo_symbol} from Yahoo Finance...")

            # Fetch data using Yahoo Finance
            data = yf.download(
                yahoo_symbol,
                start=start_date,
                end=end_date,
                progress=False,
                auto_adjust=True,
            )

            if data.empty:
                logger.warning("⚠️ No data received from Yahoo Finance for {symbol}")
                self.stats["yahoo_failures"] += 1
                return None, ValidationResult(is_valid=False, issues=[])

            # Normalize Yahoo Finance data format
            data = self._normalize_yahoo_data(data, symbol)

            # Validate Yahoo Finance data
            validation_result = validate_yahoo_finance_data(
                data, symbol, strict_mode=self.validation_strict
            )

            if validation_result.is_valid:
                self.stats["yahoo_success"] += 1
                self.stats["validation_passes"] += 1
                logger.info("✅ Yahoo Finance data validated for {symbol}")
            else:
                self.stats["yahoo_failures"] += 1
                self.stats["validation_failures"] += 1
                logger.warning("❌ Yahoo Finance data validation failed for {symbol}")

            return data, validation_result

        except Exception as e:
            logger.error("💥 Yahoo Finance fetch failed for {symbol}: {e}")
            self.stats["yahoo_failures"] += 1
            return None, ValidationResult(is_valid=False, issues=[])

    def _normalize_yahoo_data(self, data: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Normalize Yahoo Finance data format."""
        if data.empty:
            return data

        # Handle multi-level columns
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = [col[0].lower() for col in data.columns]
        else:
            data.columns = [str(col).lower() for col in data.columns]

        return data

    def cross_validate_sources(
        self,
        kite_data: pd.DataFrame,
        yahoo_data: pd.DataFrame,
        symbol: str,
        tolerance: float = 0.05,
    ) -> dict[str, Any]:
        """
        Cross-validate data from both sources.

        Args:
            kite_data: DataFrame from KiteConnect
            yahoo_data: DataFrame from Yahoo Finance
            symbol: Stock symbol
            tolerance: Acceptable difference percentage

        Returns:
            Cross-validation report
        """
        if kite_data.empty or yahoo_data.empty:
            return {
                "has_discrepancies": False,
                "reason": "Insufficient data for comparison",
                "recommendation": "Use single available source",
            }

        # Align data on common dates
        common_dates = kite_data.index.intersection(yahoo_data.index)
        if len(common_dates) == 0:
            return {
                "has_discrepancies": False,
                "reason": "No overlapping dates",
                "recommendation": "Use most recent source",
            }

        kite_common = kite_data.loc[common_dates]
        yahoo_common = yahoo_data.loc[common_dates]

        discrepancies = {}

        # Compare close prices
        if "close" in kite_common.columns and "close" in yahoo_common.columns:
            price_diff = (
                abs(kite_common["close"] - yahoo_common["close"]) / kite_common["close"]
            )
            large_diffs = price_diff[price_diff > tolerance]

            if len(large_diffs) > 0:
                discrepancies["price"] = {
                    "count": len(large_diffs),
                    "max_dif": large_diffs.max() * 100,
                    "avg_dif": large_diffs.mean() * 100,
                }

        # Compare volumes
        if "volume" in kite_common.columns and "volume" in yahoo_common.columns:
            vol_tolerance = 0.20  # 20% tolerance for volume
            vol_diff = abs(kite_common["volume"] - yahoo_common["volume"]) / (
                kite_common["volume"] + 1
            )
            large_vol_diffs = vol_diff[vol_diff > vol_tolerance]

            if len(large_vol_diffs) > 0:
                discrepancies["volume"] = {
                    "count": len(large_vol_diffs),
                    "max_dif": large_vol_diffs.max() * 100,
                }

        # Generate recommendation
        has_discrepancies = len(discrepancies) > 0
        if has_discrepancies:
            recommendation = (
                "Prefer KiteConnect data (more accurate for Indian markets)"
            )
        else:
            recommendation = "Both sources agree - either can be used"

        return {
            "has_discrepancies": has_discrepancies,
            "discrepancies": discrepancies,
            "common_dates": len(common_dates),
            "recommendation": recommendation,
        }

    def impute_missing_data(
        self,
        data: pd.DataFrame,
        symbol: str,
        method: ImputationMethod = ImputationMethod.LINEAR_INTERPOLATION,
        reference_data: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """
        Intelligent data imputation for missing values and gaps.

        Args:
            data: DataFrame with missing data
            symbol: Stock symbol
            method: Imputation method to use
            reference_data: Optional reference data for cross-source imputation

        Returns:
            DataFrame with imputed data
        """
        if data.empty:
            return data

        imputed_data = data.copy()
        imputation_performed = False

        # Required columns for OHLCV
        price_columns = ["open", "high", "low", "close"]

        for col in price_columns:
            if col not in imputed_data.columns:
                continue

            missing_mask = imputed_data[col].isna()
            if not missing_mask.any():
                continue

            logger.info(
                "🔧 Imputing {missing_mask.sum()} missing {col} values for {symbol}"
            )

            if method == ImputationMethod.FORWARD_FILL:
                imputed_data[col] = imputed_data[col].fillna(method="ffill")

            elif method == ImputationMethod.BACKWARD_FILL:
                imputed_data[col] = imputed_data[col].fillna(method="bfill")

            elif method == ImputationMethod.LINEAR_INTERPOLATION:
                imputed_data[col] = imputed_data[col].interpolate(method="linear")

            elif method == ImputationMethod.SPLINE_INTERPOLATION:
                if len(imputed_data) > 3:  # Need at least 4 points for spline
                    imputed_data[col] = imputed_data[col].interpolate(
                        method="spline", order=2
                    )
                else:
                    imputed_data[col] = imputed_data[col].interpolate(method="linear")

            elif (
                method == ImputationMethod.CROSS_SOURCE_MERGE
                and reference_data is not None
            ):
                # Use reference data to fill gaps
                common_dates = imputed_data.index.intersection(reference_data.index)
                if len(common_dates) > 0 and col in reference_data.columns:
                    for date in common_dates:
                        if pd.isna(imputed_data.loc[date, col]) and not pd.isna(
                            reference_data.loc[date, col]
                        ):
                            imputed_data.loc[date, col] = reference_data.loc[date, col]

            imputation_performed = True

        # Handle volume separately (can use different logic)
        if "volume" in imputed_data.columns:
            volume_missing = imputed_data["volume"].isna()
            if volume_missing.any():
                # For volume, use median of recent values
                recent_median = (
                    imputed_data["volume"].rolling(window=10, min_periods=1).median()
                )
                imputed_data["volume"] = imputed_data["volume"].fillna(recent_median)
                imputation_performed = True

        # Ensure OHLC consistency after imputation
        if imputation_performed:
            imputed_data = self._ensure_ohlc_consistency(imputed_data)
            self.stats["imputation_events"] += 1
            logger.info("✅ Data imputation completed for {symbol}")

        return imputed_data

    def _ensure_ohlc_consistency(self, data: pd.DataFrame) -> pd.DataFrame:
        """Ensure OHLC relationships are maintained after imputation."""
        if data.empty:
            return data

        consistent_data = data.copy()
        price_cols = ["open", "high", "low", "close"]

        # Check if all price columns exist
        if not all(col in consistent_data.columns for col in price_cols):
            return consistent_data

        for idx in consistent_data.index:
            row = consistent_data.loc[idx]

            # Ensure high is the maximum
            max_price = max(row["open"], row["close"])
            if row["high"] < max_price:
                consistent_data.loc[idx, "high"] = max_price

            # Ensure low is the minimum
            min_price = min(row["open"], row["close"])
            if row["low"] > min_price:
                consistent_data.loc[idx, "low"] = min_price

        return consistent_data

    def fetch_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        interval: str = "day",
        impute_missing: bool = True,
    ) -> dict[str, Any]:
        """
        Main method to fetch data using tiered strategy.

        Args:
            symbol: Stock symbol (without .NS suffix)
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            interval: Data interval
            impute_missing: Whether to perform data imputation

        Returns:
            Dictionary with data, validation results, and metadata
        """
        cache_key = "{symbol}_{start_date}_{end_date}_{interval}"

        # Check cache first
        if cache_key in self.cache:
            cache_entry = self.cache[cache_key]
            if (
                datetime.now() - cache_entry["timestamp"]
            ).seconds < self.cache_duration:
                logger.info("📦 Using cached data for {symbol}")
                return cache_entry["data"]

        result = {
            "symbol": symbol,
            "start_date": start_date,
            "end_date": end_date,
            "data": None,
            "data_source": None,
            "quality": DataQuality.UNUSABLE,
            "validation_result": None,
            "cross_validation": None,
            "imputation_applied": False,
            "fetch_timestamp": datetime.now(),
            "issues": [],
        }

        kite_data, kite_validation = None, None
        yahoo_data, yahoo_validation = None, None

        # Strategy: KITE_PREFERRED (recommended)
        if self.strategy == FetchStrategy.KITE_PREFERRED:
            # Try KiteConnect first
            kite_data, kite_validation = self.fetch_kite_data(
                symbol, start_date, end_date, interval
            )

            if kite_data is not None and kite_validation.is_valid:
                # KiteConnect data is good
                result["data"] = kite_data
                result["data_source"] = DataSource.KITECONNECT
                result["validation_result"] = kite_validation
                result["quality"] = self._assess_data_quality(kite_validation)

            else:
                # Fallback to Yahoo Finance
                logger.info("🔄 Falling back to Yahoo Finance for {symbol}")
                yahoo_data, yahoo_validation = self.fetch_yahoo_data(
                    symbol, start_date, end_date
                )

                if yahoo_data is not None:
                    result["data"] = yahoo_data
                    result["data_source"] = DataSource.YAHOO_FINANCE
                    result["validation_result"] = yahoo_validation
                    result["quality"] = self._assess_data_quality(yahoo_validation)

        # Strategy: BEST_QUALITY (fetch from both, use best)
        elif self.strategy == FetchStrategy.BEST_QUALITY:
            # Fetch from both sources
            kite_data, kite_validation = self.fetch_kite_data(
                symbol, start_date, end_date, interval
            )
            yahoo_data, yahoo_validation = self.fetch_yahoo_data(
                symbol, start_date, end_date
            )

            # Choose the best quality data
            kite_quality = (
                self._assess_data_quality(kite_validation)
                if kite_validation
                else DataQuality.UNUSABLE
            )
            yahoo_quality = (
                self._assess_data_quality(yahoo_validation)
                if yahoo_validation
                else DataQuality.UNUSABLE
            )

            quality_ranking = {
                DataQuality.EXCELLENT: 5,
                DataQuality.GOOD: 4,
                DataQuality.FAIR: 3,
                DataQuality.POOR: 2,
                DataQuality.UNUSABLE: 1,
            }

            if quality_ranking[kite_quality] >= quality_ranking[yahoo_quality]:
                result["data"] = kite_data
                result["data_source"] = DataSource.KITECONNECT
                result["validation_result"] = kite_validation
                result["quality"] = kite_quality
            else:
                result["data"] = yahoo_data
                result["data_source"] = DataSource.YAHOO_FINANCE
                result["validation_result"] = yahoo_validation
                result["quality"] = yahoo_quality

        # Strategy: REDUNDANT (fetch from both, cross-validate)
        elif self.strategy == FetchStrategy.REDUNDANT:
            kite_data, kite_validation = self.fetch_kite_data(
                symbol, start_date, end_date, interval
            )
            yahoo_data, yahoo_validation = self.fetch_yahoo_data(
                symbol, start_date, end_date
            )

            # Cross-validate if both available
            if kite_data is not None and yahoo_data is not None:
                result["cross_validation"] = self.cross_validate_sources(
                    kite_data, yahoo_data, symbol
                )

            # Prefer KiteConnect, but use cross-validation for imputation
            if kite_data is not None:
                result["data"] = kite_data
                result["data_source"] = DataSource.KITECONNECT
                result["validation_result"] = kite_validation
                result["quality"] = self._assess_data_quality(kite_validation)

        # Apply imputation if requested and data has issues
        if (
            impute_missing
            and result["data"] is not None
            and result["quality"] in [DataQuality.FAIR, DataQuality.POOR]
        ):

            reference_data = (
                yahoo_data
                if result["data_source"] == DataSource.KITECONNECT
                else kite_data
            )

            result["data"] = self.impute_missing_data(
                result["data"],
                symbol,
                method=ImputationMethod.LINEAR_INTERPOLATION,
                reference_data=reference_data,
            )
            result["imputation_applied"] = True

            # Re-validate after imputation
            post_imputation_validation = self.validator.comprehensive_validation(
                result["data"], symbol, result["data_source"]
            )
            result["validation_result"] = post_imputation_validation
            result["quality"] = self._assess_data_quality(post_imputation_validation)

        # Cache the result
        self.cache[cache_key] = {"data": result, "timestamp": datetime.now()}

        # Log final result
        if result["data"] is not None:
            logger.info(
                "✅ Data fetch completed for {symbol} - Source: {result['data_source'].value}, Quality: {result['quality'].value}"
            )
        else:
            logger.error("❌ Data fetch failed for {symbol}")
            result["issues"].append("All data sources failed")

        return result

    def _assess_data_quality(self, validation_result: ValidationResult) -> DataQuality:
        """Assess overall data quality based on validation results."""
        if not validation_result or not validation_result.is_valid:
            return DataQuality.UNUSABLE

        issues = validation_result.issues
        critical_issues = sum(
            1 for i in issues if i.severity == ValidationSeverity.CRITICAL
        )
        error_issues = sum(1 for i in issues if i.severity == ValidationSeverity.ERROR)
        warning_issues = sum(
            1 for i in issues if i.severity == ValidationSeverity.WARNING
        )

        if critical_issues > 0:
            return DataQuality.UNUSABLE
        elif error_issues > 2:
            return DataQuality.POOR
        elif error_issues > 0 or warning_issues > 5:
            return DataQuality.FAIR
        elif warning_issues > 0:
            return DataQuality.GOOD
        else:
            return DataQuality.EXCELLENT

    def get_statistics(self) -> dict[str, Any]:
        """Get fetcher statistics."""
        total_requests = (
            self.stats["kite_success"]
            + self.stats["kite_failures"]
            + self.stats["yahoo_success"]
            + self.stats["yahoo_failures"]
        )

        if total_requests == 0:
            return self.stats

        return {
            **self.stats,
            "kite_success_rate": (
                self.stats["kite_success"]
                / (self.stats["kite_success"] + self.stats["kite_failures"])
                * 100
                if (self.stats["kite_success"] + self.stats["kite_failures"]) > 0
                else 0
            ),
            "yahoo_success_rate": (
                self.stats["yahoo_success"]
                / (self.stats["yahoo_success"] + self.stats["yahoo_failures"])
                * 100
                if (self.stats["yahoo_success"] + self.stats["yahoo_failures"]) > 0
                else 0
            ),
            "overall_success_rate": (
                self.stats["kite_success"] + self.stats["yahoo_success"]
            )
            / total_requests
            * 100,
            "validation_success_rate": (
                self.stats["validation_passes"]
                / (self.stats["validation_passes"] + self.stats["validation_failures"])
                * 100
                if (self.stats["validation_passes"] + self.stats["validation_failures"])
                > 0
                else 0
            ),
        }

    def reset_statistics(self):
        """Reset statistics counters."""
        for key in self.stats:
            if key != "last_reset":
                self.stats[key] = 0
        self.stats["last_reset"] = datetime.now()


# Convenience functions
def fetch_with_validation(
    symbol: str,
    start_date: str,
    end_date: str,
    kite_api_key: Optional[str] = None,
    kite_access_token: Optional[str] = None,
    strategy: FetchStrategy = FetchStrategy.KITE_PREFERRED,
) -> dict[str, Any]:
    """
    Convenience function to fetch data with validation.

    Args:
        symbol: Stock symbol
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        kite_api_key: Optional KiteConnect API key
        kite_access_token: Optional KiteConnect access token
        strategy: Fetching strategy

    Returns:
        Data fetch result
    """
    fetcher = TieredDataFetcher(
        kite_api_key=kite_api_key,
        kite_access_token=kite_access_token,
        strategy=strategy,
    )

    return fetcher.fetch_data(symbol, start_date, end_date)


if __name__ == "__main__":
    # Example usage
    print("🔧 Tiered Data Fetching System")
    print("=" * 40)

    # Test without KiteConnect (using Yahoo only)
    fetcher = TieredDataFetcher(strategy=FetchStrategy.YAHOO_ONLY)

    result = fetcher.fetch_data(
        symbol="RELIANCE", start_date="2025-06-01", end_date="2025-06-28"
    )

    print("Result for RELIANCE:")
    print(
        "- Data source: {result['data_source'].value if result['data_source'] else 'None'}"
    )
    print("- Quality: {result['quality'].value}")
    print(
        "- Shape: {result['data'].shape if result['data'] is not None else 'No data'}"
    )
    print("- Imputation applied: {result['imputation_applied']}")

    stats = fetcher.get_statistics()
    print("\nStatistics:")
    print("- Overall success rate: {stats['overall_success_rate']:.1f}%")
    print("- Validation success rate: {stats['validation_success_rate']:.1f}%")
