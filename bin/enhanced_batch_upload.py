#!/usr/bin/env python3
"""
Enhanced Batch Upload for Historical Data (2010-2025)
- Uses Kite API for better data quality
- Faster processing with parallel uploads
- Extended date range from 2010 to current date
- Better error handling and validation
"""

import argparse
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf
from google.cloud import bigquery
from kiteconnect import KiteConnect

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class EnhancedDataFetcher:
    def __init__(self, use_kite=True):
        self.use_kite = use_kite
        self.kite = None

        if use_kite:
            self.setup_kite()

    def setup_kite(self):
        """Setup Kite Connect if API key is available"""
        try:
            api_key = os.getenv("KITE_API_KEY")
            if api_key:
                self.kite = KiteConnect(api_key=api_key)
                logger.info("✅ Kite Connect initialized")
            else:
                logger.warning("⚠️ KITE_API_KEY not found, using yfinance only")
                self.use_kite = False
        except Exception as e:
            logger.warning(f"⚠️ Kite setup failed: {e}, using yfinance")
            self.use_kite = False

    def fetch_data(self, symbol, start_date, end_date):
        """Fetch data using Kite API or yfinance"""
        if self.use_kite and self.kite:
            return self.fetch_from_kite(symbol, start_date, end_date)
        else:
            return self.fetch_from_yfinance(symbol, start_date, end_date)

    def fetch_from_kite(self, symbol, start_date, end_date):
        """Fetch data from Kite API"""
        try:
            # Convert symbol format for Kite (e.g., RELIANCE.NS -> RELIANCE)
            kite_symbol = symbol.replace(".NS", "")

            # Fetch historical data
            data = self.kite.historical_data(
                instrument_token=kite_symbol,
                from_date=start_date,
                to_date=end_date,
                interval="day",
            )

            if not data:
                return None

            # Convert to DataFrame
            df = pd.DataFrame(data)
            df["Date"] = pd.to_datetime(df["date"])
            df = df.set_index("Date")
            df.columns = ["Open", "High", "Low", "Close", "Volume"]

            return df

        except Exception as e:
            logger.warning(f"⚠️ Kite fetch failed for {symbol}: {e}")
            return self.fetch_from_yfinance(symbol, start_date, end_date)

    def fetch_from_yfinance(self, symbol, start_date, end_date):
        """Fetch data from yfinance"""
        try:
            ticker = yf.Ticker(symbol)
            data = ticker.history(start=start_date, end=end_date)
            return data if not data.empty else None
        except Exception as e:
            logger.error(f"❌ yfinance fetch failed for {symbol}: {e}")
            return None


def load_symbols(config_path):
    """Load symbols from config file"""
    with open(config_path) as f:
        config = json.load(f)

    # Check different possible structures
    if "symbols" in config:
        return config["symbols"]
    elif "sectors" in config:
        symbols = []
        for sector, tickers in config["sectors"].items():
            symbols.extend(tickers)
        return symbols
    elif "tickers" in config:
        return config["tickers"]
    else:
        return []


def generate_monthly_ranges(start_date, end_date):
    """Generate monthly date ranges"""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    ranges = []
    current = start

    while current < end:
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1, day=1)
        else:
            next_month = current.replace(month=current.month + 1, day=1)

        range_end = min(next_month - timedelta(days=1), end)
        ranges.append((current.strftime("%Y-%m-%d"), range_end.strftime("%Y-%m-%d")))
        current = next_month

    return ranges


def fetch_and_upload_symbol(
    symbol, start_date, end_date, table_id, client, data_fetcher
):
    """Fetch data for symbol and upload to BigQuery"""
    try:
        logger.info(f"📈 Processing {symbol} for {start_date} to {end_date}")

        # Fetch data using enhanced fetcher
        data = data_fetcher.fetch_data(symbol, start_date, end_date)

        if data is None or data.empty:
            logger.warning(f"⚠️ No data for {symbol}")
            return 0

        # Prepare data for BigQuery
        data = data.reset_index()
        data["symbol"] = symbol
        data["timestamp"] = pd.Timestamp.now()
        data["data_source"] = "kite" if data_fetcher.use_kite else "yfinance"
        data["data_quality_score"] = 1.0
        data["created_at"] = pd.Timestamp.now()
        data["sector"] = "Unknown"

        # Determine market cap segment from table name
        if "large_cap" in table_id:
            data["market_cap_segment"] = "large_cap"
        elif "mid_cap" in table_id:
            data["market_cap_segment"] = "mid_cap"
        else:
            data["market_cap_segment"] = "small_cap"

        # Rename columns to match BigQuery schema
        column_mapping = {
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
        data = data.rename(columns=column_mapping)

        # Add adjusted_close (same as close for now)
        data["adjusted_close"] = data["close"]

        # Select required columns only
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

        # Convert date to date type
        data["date"] = pd.to_datetime(data["date"]).dt.date

        # Upload to BigQuery
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        )

        job = client.load_table_from_dataframe(data, table_id, job_config=job_config)
        job.result()  # Wait for job to complete

        logger.info(f"✅ Uploaded {len(data)} records for {symbol}")
        return len(data)

    except Exception as e:
        logger.error(f"❌ Error processing {symbol}: {str(e)}")
        return 0


def process_batch_parallel(
    symbols, start_date, end_date, table_id, client, data_fetcher, max_workers=5
):
    """Process a batch of symbols in parallel"""
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_symbol = {
            executor.submit(
                fetch_and_upload_symbol,
                symbol,
                start_date,
                end_date,
                table_id,
                client,
                data_fetcher,
            ): symbol
            for symbol in symbols
        }

        # Collect results
        for future in as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            try:
                records = future.result()
                results.append((symbol, records))
            except Exception as e:
                logger.error(f"❌ Error in parallel processing for {symbol}: {e}")
                results.append((symbol, 0))

    return results


def process_segment(segment, config_path, start_date, end_date, project_id):
    """Process an entire market segment with enhanced speed"""
    logger.info(f"🎯 Processing {segment} segment")

    # Initialize BigQuery client and data fetcher
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.trading_data.{segment}_price_data"
    data_fetcher = EnhancedDataFetcher(use_kite=True)

    # Load symbols
    symbols = load_symbols(config_path)
    logger.info(f"📈 Processing {len(symbols)} symbols")

    # Generate monthly ranges
    date_ranges = generate_monthly_ranges(start_date, end_date)
    logger.info(f"📅 Processing {len(date_ranges)} monthly batches")

    total_records = 0
    successful_symbols = set()

    # Process each month with parallel processing
    for i, (range_start, range_end) in enumerate(date_ranges, 1):
        logger.info(
            f"🗓️ Processing batch {i}/{len(date_ranges)}: {range_start} to {range_end}"
        )

        # Process batch in parallel
        batch_results = process_batch_parallel(
            symbols,
            range_start,
            range_end,
            table_id,
            client,
            data_fetcher,
            max_workers=5,
        )

        # Calculate batch statistics
        batch_records = sum(records for _, records in batch_results)
        batch_successful = sum(1 for _, records in batch_results if records > 0)

        for symbol, records in batch_results:
            if records > 0:
                successful_symbols.add(symbol)

        logger.info(
            f"✅ Batch {i} complete: {batch_successful}/{len(symbols)} symbols, {batch_records:,} records"
        )
        total_records += batch_records

        # Reduced delay between months (from 15s to 5s)
        if i < len(date_ranges):
            logger.info("⏸️ Waiting 5 seconds before next batch...")
            time.sleep(5)

    logger.info(f"🎯 {segment} segment completed:")
    logger.info(f"  ✅ Successful symbols: {len(successful_symbols)}/{len(symbols)}")
    logger.info(f"  📊 Total records uploaded: {total_records:,}")

    return len(successful_symbols), total_records


def validate_bq_data(project_id, segment):
    """Validate data in BigQuery"""
    try:
        client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.trading_data.{segment}_price_data"

        # Check row count
        query = f"""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT symbol) as unique_symbols,
            MIN(date) as earliest_date,
            MAX(date) as latest_date
        FROM `{table_id}`
        """

        result = client.query(query).result()
        for row in result:
            logger.info(f"📊 {segment} validation:")
            logger.info(f"  Total rows: {row.total_rows:,}")
            logger.info(f"  Unique symbols: {row.unique_symbols}")
            logger.info(f"  Date range: {row.earliest_date} to {row.latest_date}")

        return True

    except Exception as e:
        logger.error(f"❌ Validation failed for {segment}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Enhanced batch upload historical data (2010-2025)"
    )
    parser.add_argument("--segments", required=True, help="Comma-separated segments")
    parser.add_argument(
        "--start-date", default="2010-01-01", help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument("--project-id", required=True, help="BigQuery project ID")
    parser.add_argument(
        "--validate", action="store_true", help="Validate data after upload"
    )

    args = parser.parse_args()

    # Segment configurations
    segment_configs = {
        "large_cap": "config/market_cap/nifty_100_large_cap.json",
        "mid_cap": "config/market_cap/nifty_midcap_selected.json",
        "small_cap": "config/market_cap/nifty_smallcap_selected.json",
    }

    segments = args.segments.split(",")

    logger.info("🚀 Starting Enhanced Batch Historical Data Upload")
    logger.info(f"📅 Date Range: {args.start_date} to {args.end_date}")
    logger.info(f"📊 Project: {args.project_id}")
    logger.info(f"🔄 Segments: {', '.join(segments)}")
    logger.info("=" * 60)

    total_successful = 0
    total_records = 0

    for segment in segments:
        if segment in segment_configs:
            config_path = segment_configs[segment]
            successful, records = process_segment(
                segment, config_path, args.start_date, args.end_date, args.project_id
            )
            total_successful += successful
            total_records += records

            # Validate if requested
            if args.validate:
                validate_bq_data(args.project_id, segment)
        else:
            logger.error(f"❌ Unknown segment: {segment}")

    logger.info("=" * 60)
    logger.info("📋 ENHANCED BATCH UPLOAD COMPLETION REPORT")
    logger.info(f"✅ Total successful symbols: {total_successful}")
    logger.info(f"📊 Total records uploaded: {total_records:,}")
    logger.info(f"📅 Date range processed: {args.start_date} to {args.end_date}")
    logger.info("✅ ENHANCED BATCH UPLOAD COMPLETED")


if __name__ == "__main__":
    main()
