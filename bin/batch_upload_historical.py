#!/usr/bin/env python3
"""
Batch Upload Historical Data
Uploads historical data in monthly batches to avoid BigQuery partition limits.
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            f'logs/batch_upload_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        ),
    ],
)
logger = logging.getLogger(__name__)


class BatchHistoricalUploader:
    def __init__(self, project_id: str, dataset_id: str = "trading_data"):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq_connector = BigQueryConnector(project_id, dataset_id)
        self.yf_loader = YFinanceLoader(validation_enabled=True)
        self.validator = DataValidator()

    def load_symbol_config(self, config_path: str) -> list:
        """Load symbols from config file"""
        with open(config_path) as f:
            config = json.load(f)
        return config.get("symbols", [])

    def generate_monthly_ranges(self, start_date: str, end_date: str) -> list:
        """Generate monthly date ranges to avoid partition limits"""
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        ranges = []
        current = start

        while current < end:
            # Calculate end of current month
            if current.month == 12:
                next_month = current.replace(year=current.year + 1, month=1, day=1)
            else:
                next_month = current.replace(month=current.month + 1, day=1)

            range_end = min(next_month - timedelta(days=1), end)
            ranges.append(
                (current.strftime("%Y-%m-%d"), range_end.strftime("%Y-%m-%d"))
            )
            current = next_month

        return ranges

    def upload_symbol_batch(
        self, symbol: str, start_date: str, end_date: str, table_name: str
    ) -> dict:
        """Upload data for a single symbol in the given date range"""
        try:
            logger.info(f"📈 Processing {symbol} for {start_date} to {end_date}")

            # Fetch data
            data = self.yf_loader.fetch_data(symbol, start_date, end_date)
            if data is None or data.empty:
                logger.warning(
                    f"⚠️ No data for {symbol} in range {start_date} to {end_date}"
                )
                return {"symbol": symbol, "status": "no_data", "records": 0}

            # Validate data
            validation_result = self.validator.validate_data(
                data, symbol, DataSource.YFINANCE
            )
            if not validation_result.is_valid:
                logger.warning(f"⚠️ Data validation failed for {symbol}")
                return {"symbol": symbol, "status": "validation_failed", "records": 0}

            # Prepare data for BigQuery
            data = data.reset_index()
            data["symbol"] = symbol
            data["timestamp"] = pd.Timestamp.now()
            data["data_source"] = "yfinance"
            data["data_quality_score"] = validation_result.quality_score
            data["created_at"] = pd.Timestamp.now()

            # Add sector info (simplified for now)
            data["sector"] = "Unknown"
            data["market_cap_segment"] = table_name.replace("_price_data", "")

            # Rename columns to match BigQuery schema
            column_mapping = {
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
                "Adj Close": "adjusted_close",
            }
            data = data.rename(columns=column_mapping)

            # Select only required columns
            required_cols = [
                "symbol",
                "date",
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "adjusted_close",
                "sector",
                "market_cap_segment",
                "data_source",
                "data_quality_score",
                "created_at",
            ]
            data = data[required_cols]

            # Upload to BigQuery
            success = self.bq_connector.save_data(data, table_name)

            if success:
                logger.info(f"✅ Uploaded {len(data)} records for {symbol}")
                return {"symbol": symbol, "status": "success", "records": len(data)}
            else:
                logger.error(f"❌ Upload failed for {symbol}")
                return {"symbol": symbol, "status": "upload_failed", "records": 0}

        except Exception as e:
            logger.error(f"❌ Error processing {symbol}: {str(e)}")
            return {"symbol": symbol, "status": "error", "records": 0, "error": str(e)}

    def process_segment(
        self, segment: str, config_path: str, start_date: str, end_date: str
    ):
        """Process an entire market segment"""
        logger.info(f"🎯 Processing {segment} segment")
        logger.info(f"📋 Config: {config_path}")

        # Load symbols
        symbols = self.load_symbol_config(config_path)
        logger.info(f"📈 Processing {len(symbols)} symbols")

        # Generate monthly ranges
        date_ranges = self.generate_monthly_ranges(start_date, end_date)
        logger.info(f"📅 Processing {len(date_ranges)} monthly batches")

        table_name = f"{segment}_price_data"
        total_results = []

        # Process each month
        for i, (range_start, range_end) in enumerate(date_ranges, 1):
            logger.info(
                f"🗓️ Processing batch {i}/{len(date_ranges)}: {range_start} to {range_end}"
            )

            batch_results = []
            for symbol in symbols:
                result = self.upload_symbol_batch(
                    symbol, range_start, range_end, table_name
                )
                batch_results.append(result)

                # Small delay to avoid rate limits
                import time

                time.sleep(0.1)

            # Log batch summary
            successful = sum(1 for r in batch_results if r["status"] == "success")
            total_records = sum(r["records"] for r in batch_results)
            logger.info(
                f"✅ Batch {i} complete: {successful}/{len(symbols)} symbols, {total_records:,} records"
            )

            total_results.extend(batch_results)

            # Delay between months to be respectful to APIs
            if i < len(date_ranges):
                logger.info("⏸️ Waiting 10 seconds before next batch...")
                time.sleep(10)

        # Final summary
        successful_symbols = {
            r["symbol"] for r in total_results if r["status"] == "success"
        }
        total_records = sum(
            r["records"] for r in total_results if r["status"] == "success"
        )

        logger.info(f"🎯 {segment} segment completed:")
        logger.info(
            f"  ✅ Successful symbols: {len(successful_symbols)}/{len(symbols)}"
        )
        logger.info(f"  📊 Total records uploaded: {total_records:,}")

        return total_results


def main():
    parser = argparse.ArgumentParser(
        description="Batch upload historical data to BigQuery"
    )
    parser.add_argument(
        "--segments",
        required=True,
        help="Comma-separated segments (large_cap,mid_cap,small_cap)",
    )
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--project-id", required=True, help="BigQuery project ID")
    parser.add_argument(
        "--dataset-id", default="trading_data", help="BigQuery dataset ID"
    )

    args = parser.parse_args()

    # Initialize uploader
    uploader = BatchHistoricalUploader(args.project_id, args.dataset_id)

    # Segment configurations
    segment_configs = {
        "large_cap": "config/market_cap/nifty_100_large_cap.json",
        "mid_cap": "config/market_cap/nifty_midcap_selected.json",
        "small_cap": "config/market_cap/nifty_smallcap_selected.json",
    }

    # Process requested segments
    segments = args.segments.split(",")

    logger.info("🚀 Starting Batch Historical Data Upload")
    logger.info(f"📅 Date Range: {args.start_date} to {args.end_date}")
    logger.info(f"📊 Project: {args.project_id}")
    logger.info(f"🗄️ Dataset: {args.dataset_id}")
    logger.info("=" * 60)

    all_results = {}

    for segment in segments:
        if segment in segment_configs:
            config_path = segment_configs[segment]
            if os.path.exists(config_path):
                results = uploader.process_segment(
                    segment, config_path, args.start_date, args.end_date
                )
                all_results[segment] = results
            else:
                logger.error(f"❌ Config file not found: {config_path}")
        else:
            logger.error(f"❌ Unknown segment: {segment}")

    # Final report
    logger.info("=" * 60)
    logger.info("📋 BATCH UPLOAD COMPLETION REPORT")
    logger.info("=" * 60)

    for segment, results in all_results.items():
        successful = sum(1 for r in results if r["status"] == "success")
        total_symbols = len({r["symbol"] for r in results})
        total_records = sum(r["records"] for r in results if r["status"] == "success")

        logger.info(f"{segment.upper()}:")
        logger.info(f"  Success Rate: {successful}/{total_symbols} symbols")
        logger.info(f"  Total Records: {total_records:,}")

    logger.info("✅ BATCH UPLOAD COMPLETED")


if __name__ == "__main__":
    main()
