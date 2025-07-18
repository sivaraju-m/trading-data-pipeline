#!/usr/bin/env python3
"""
Complete Historical Data Pipeline Execution
===========================================
Executes the complete historical data pipeline as per PLAN.md
- Pulls data for entire universe (large, mid, small cap)
- Uses KiteConnect primary and yfinance fallback
- Uploads directly to BigQuery
- Implements comprehensive validation and monitoring

Author: AI Trading Machine
Licensed by SJ Trading
"""

import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

# Setup environment
os.environ["GCP_PROJECT_ID"] = "ai-trading-gcp-459813"
os.environ["BQ_DATASET"] = "trading_data"
os.environ["BQ_LOCATION"] = "us-central1"

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from flow.history_data_pull import HistoryDataPuller

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(
            f'logs/complete_pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        ),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


class CompleteHistoricalPipeline:
    """Complete historical data pipeline implementation"""

    def __init__(self):
        self.project_id = "ai-trading-gcp-459813"
        self.dataset = "trading_data"
        self.puller = HistoryDataPuller()

        # Market cap segments configuration
        self.segments = {
            "large_cap": {
                "config_file": "config/market_cap/nifty_100_large_cap.json",
                "table_name": "large_cap_price_data",
                "description": "NIFTY 100 Large Cap Stocks",
            },
            "mid_cap": {
                "config_file": "config/market_cap/nifty_midcap_selected.json",
                "table_name": "mid_cap_price_data",
                "description": "NIFTY Midcap Selected Stocks",
            },
            "small_cap": {
                "config_file": "config/market_cap/nifty_smallcap_selected.json",
                "table_name": "small_cap_price_data",
                "description": "NIFTY Smallcap Selected Stocks",
            },
        }

        # Pipeline results
        self.results = {
            "pipeline_start": datetime.now().isoformat(),
            "segments_processed": {},
            "total_symbols": 0,
            "total_successful": 0,
            "total_failed": 0,
            "errors": [],
        }

    def load_universe_symbols(self, config_file: str) -> list[str]:
        """Load symbols from market cap configuration file"""
        try:
            config_path = project_root / config_file
            if not config_path.exists():
                raise FileNotFoundError(f"Config file not found: {config_path}")

            with open(config_path) as f:
                config = json.load(f)

            symbols = []

            # Extract symbols from sectors structure
            if "sectors" in config:
                for sector, sector_symbols in config["sectors"].items():
                    symbols.extend(sector_symbols)

            # Extract symbols from universe structure (alternative format)
            elif "universe" in config:
                for stock in config["universe"]:
                    if isinstance(stock, dict) and "symbol" in stock:
                        symbols.append(stock["symbol"])
                    elif isinstance(stock, str):
                        symbols.append(stock)

            # If symbols are directly in the config
            elif "symbols" in config:
                symbols = config["symbols"]

            # Remove duplicates and clean symbols
            symbols = list(set(symbols))
            logger.info(f"Loaded {len(symbols)} symbols from {config_file}")

            return symbols

        except Exception as e:
            logger.error(f"Failed to load symbols from {config_file}: {e}")
            return []

    def process_segment(
        self, segment: str, start_date: str, end_date: str, batch_size: int = 10
    ) -> dict:
        """Process a complete market cap segment"""
        segment_config = self.segments[segment]

        logger.info(f"🎯 Processing {segment} segment")
        logger.info(f"📋 Config: {segment_config['config_file']}")
        logger.info(f"📅 Period: {start_date} to {end_date}")

        # Load symbols for this segment
        symbols = self.load_universe_symbols(segment_config["config_file"])

        if not symbols:
            error_msg = f"No symbols found for {segment} segment"
            logger.error(error_msg)
            return {
                "segment": segment,
                "status": "failed",
                "error": error_msg,
                "symbols_requested": 0,
                "symbols_successful": 0,
                "symbols_failed": 0,
            }

        logger.info(f"📈 Processing {len(symbols)} symbols in {segment} segment")

        # Process symbols in batches to avoid overwhelming the system
        total_successful = 0
        total_failed = 0
        failed_symbols = []

        for i in range(0, len(symbols), batch_size):
            batch = symbols[i : i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(symbols) + batch_size - 1) // batch_size

            logger.info(
                f"🔄 Processing batch {batch_num}/{total_batches} ({len(batch)} symbols)"
            )

            try:
                # Pull data for this batch
                results = self.puller.pull_historical_data(
                    symbols=batch,
                    start_date=start_date,
                    end_date=end_date,
                    bq_project=self.project_id,
                    bq_dataset=self.dataset,
                    bq_table=segment_config["table_name"],
                    market_cap_segment=segment,
                )

                # Update counters
                batch_successful = len(results["successful"])
                batch_failed = len(results["failed"])

                total_successful += batch_successful
                total_failed += batch_failed
                failed_symbols.extend(results["failed"])

                logger.info(
                    f"✅ Batch {batch_num}: {batch_successful} successful, {batch_failed} failed"
                )

                # Add delay between batches to respect rate limits
                if i + batch_size < len(symbols):
                    logger.info("⏸️  Waiting 30 seconds before next batch...")
                    time.sleep(30)

            except Exception as e:
                error_msg = f"Batch {batch_num} failed: {str(e)}"
                logger.error(error_msg)
                total_failed += len(batch)
                failed_symbols.extend(batch)

        segment_result = {
            "segment": segment,
            "status": "completed" if total_failed == 0 else "partial",
            "symbols_requested": len(symbols),
            "symbols_successful": total_successful,
            "symbols_failed": total_failed,
            "failed_symbols": failed_symbols[:10],  # First 10 for logging
            "table_name": segment_config["table_name"],
            "completion_time": datetime.now().isoformat(),
        }

        logger.info(
            f"🎯 {segment} segment completed: {total_successful}/{len(symbols)} successful"
        )

        return segment_result

    def validate_bigquery_data(self, segment: str) -> dict:
        """Validate uploaded data in BigQuery"""
        try:
            from google.cloud import bigquery

            client = bigquery.Client(project=self.project_id)
            table_name = self.segments[segment]["table_name"]

            # Query data statistics
            query = f"""
            SELECT
                COUNT(*) as total_records,
                COUNT(DISTINCT symbol) as unique_symbols,
                MIN(date) as earliest_date,
                MAX(date) as latest_date,
                COUNT(DISTINCT data_source) as data_sources
            FROM `{self.project_id}.{self.dataset}.{table_name}`
            """

            query_job = client.query(query)
            results = list(query_job.result())

            if results:
                row = results[0]
                validation_result = {
                    "total_records": row.total_records,
                    "unique_symbols": row.unique_symbols,
                    "earliest_date": (
                        str(row.earliest_date) if row.earliest_date else None
                    ),
                    "latest_date": str(row.latest_date) if row.latest_date else None,
                    "data_sources": row.data_sources,
                    "validation_status": "passed",
                }

                logger.info(
                    f"✅ {segment} validation: {row.total_records} records, {row.unique_symbols} symbols"
                )
                return validation_result
            else:
                return {"validation_status": "failed", "error": "No data found"}

        except Exception as e:
            logger.error(f"❌ Validation failed for {segment}: {e}")
            return {"validation_status": "failed", "error": str(e)}

    def run_complete_pipeline(
        self, start_date: str = "2010-01-01", end_date: Optional[str] = None
    ):
        """Run the complete historical data pipeline"""
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        logger.info("🚀 Starting Complete Historical Data Pipeline")
        logger.info(f"📅 Date Range: {start_date} to {end_date}")
        logger.info(f"📊 Project: {self.project_id}")
        logger.info(f"🗄️  Dataset: {self.dataset}")
        logger.info("=" * 60)

        # Process each market cap segment
        for segment in self.segments.keys():
            try:
                segment_result = self.process_segment(segment, start_date, end_date)
                self.results["segments_processed"][segment] = segment_result

                # Update totals
                self.results["total_symbols"] += segment_result["symbols_requested"]
                self.results["total_successful"] += segment_result["symbols_successful"]
                self.results["total_failed"] += segment_result["symbols_failed"]

                # Validate uploaded data
                logger.info(f"🔍 Validating {segment} data in BigQuery...")
                validation_result = self.validate_bigquery_data(segment)
                self.results["segments_processed"][segment][
                    "validation"
                ] = validation_result

            except Exception as e:
                error_msg = f"Segment {segment} failed: {str(e)}"
                logger.error(error_msg)
                self.results["errors"].append(error_msg)

        # Generate final report
        self.results["pipeline_end"] = datetime.now().isoformat()
        self.generate_final_report()

    def generate_final_report(self):
        """Generate comprehensive pipeline execution report"""
        logger.info("=" * 60)
        logger.info("📋 COMPLETE PIPELINE EXECUTION REPORT")
        logger.info("=" * 60)

        logger.info(
            f"⏰ Pipeline Duration: {self.results['pipeline_start']} to {self.results['pipeline_end']}"
        )
        logger.info(f"📊 Total Symbols Processed: {self.results['total_symbols']}")
        logger.info(f"✅ Total Successful: {self.results['total_successful']}")
        logger.info(f"❌ Total Failed: {self.results['total_failed']}")

        success_rate = (
            (self.results["total_successful"] / self.results["total_symbols"] * 100)
            if self.results["total_symbols"] > 0
            else 0
        )
        logger.info(f"📈 Success Rate: {success_rate:.2f}%")

        logger.info("\n📋 Segment Details:")
        for segment, result in self.results["segments_processed"].items():
            logger.info(f"  {segment.upper()}:")
            logger.info(f"    Status: {result['status']}")
            logger.info(
                f"    Symbols: {result['symbols_successful']}/{result['symbols_requested']}"
            )
            logger.info(f"    Table: {result['table_name']}")

            if "validation" in result:
                validation = result["validation"]
                if validation["validation_status"] == "passed":
                    logger.info(f"    Records: {validation['total_records']}")
                    logger.info(
                        f"    Date Range: {validation['earliest_date']} to {validation['latest_date']}"
                    )

        # Save detailed report
        report_file = f"reports/complete_pipeline_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        os.makedirs("reports", exist_ok=True)

        with open(report_file, "w") as f:
            json.dump(self.results, f, indent=2)

        logger.info(f"\n📄 Detailed report saved: {report_file}")

        # Status summary
        if self.results["total_failed"] == 0:
            logger.info("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
        elif success_rate >= 90:
            logger.info("✅ PIPELINE COMPLETED WITH MINOR ISSUES")
        else:
            logger.info("⚠️ PIPELINE COMPLETED WITH SIGNIFICANT ISSUES")

        logger.info("=" * 60)


def main():
    """Main execution function"""
    pipeline = CompleteHistoricalPipeline()

    # Run for recent data first (faster testing)
    # pipeline.run_complete_pipeline(start_date="2024-01-01", end_date="2024-12-31")

    # Run for complete historical data as per plan
    pipeline.run_complete_pipeline(start_date="2010-01-01")


if __name__ == "__main__":
    main()
