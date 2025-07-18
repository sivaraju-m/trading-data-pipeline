"""
Database Schema for Automated Trading System
==========================================

This module defines the database schema for BigQuery and Firestore
to store trading signals, paper trades, performance metrics, and manual trades.

Tables:
1. trading_signals - All generated signals
2. paper_trades - Automated simulation trades
3. manual_trades - User-executed trades
4. daily_performance - Daily aggregated metrics
5. portfolio_snapshots - Portfolio value over time
6. market_data - Historical price data

Author: AI Trading Machine
Licensed by SJ Trading
"""

import logging
import os
from datetime import date, datetime
from typing import Any

from google.cloud import bigquery, firestore

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages BigQuery and Firestore operations."""

    def __init__(self, project_id: str = None):
        """Initialize database connections."""
        self.project_id = project_id or os.getenv(
            "GOOGLE_CLOUD_PROJECT", "ai-trading-machine"
        )
        self.bq_client = None
        self.firestore_client = None
        self.dataset_id = "trading_data"

    def initialize_bigquery(self):
        """Initialize BigQuery client and create dataset/tables."""
        try:
            self.bq_client = bigquery.Client(project=self.project_id)

            # Create dataset if not exists
            dataset_ref = self.bq_client.dataset(self.dataset_id)
            try:
                self.bq_client.get_dataset(dataset_ref)
                logger.info("✅ BigQuery dataset {self.dataset_id} exists")
            except Exception:
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = "US"
                self.bq_client.create_dataset(dataset)
                logger.info("✅ Created BigQuery dataset {self.dataset_id}")

            # Create tables
            self._create_bigquery_tables()

        except Exception as e:
            logger.error("❌ BigQuery initialization failed: {e}")

    def initialize_firestore(self):
        """Initialize Firestore client."""
        try:
            self.firestore_client = firestore.Client(project=self.project_id)
            logger.info("✅ Firestore client initialized")
        except Exception as e:
            logger.error("❌ Firestore initialization failed: {e}")

    def _create_bigquery_tables(self):
        """Create BigQuery tables with proper schema."""

        # Trading Signals Table
        signals_schema = [
            bigquery.SchemaField("signal_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("signal_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("confidence", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("entry_price", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("target_price", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("stop_loss", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("quantity_suggestion", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("risk_reward_ratio", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("signal_time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("strategy_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("reason", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("market_data", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        # Paper Trades Table
        paper_trades_schema = [
            bigquery.SchemaField("trade_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("signal_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("action", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("quantity", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("entry_price", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("target_price", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("stop_loss", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("entry_time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("exit_time", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("exit_price", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("pnl", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("pnl_percent", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("trade_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("strategy_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("notes", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        # Manual Trades Table (for when user executes trades)
        manual_trades_schema = [
            bigquery.SchemaField("trade_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("signal_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("action", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("quantity", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("entry_price", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("target_price", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("stop_loss", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("entry_time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("exit_time", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("exit_price", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("pnl", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("pnl_percent", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("trade_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("broker_order_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("execution_notes", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        # Daily Performance Table
        daily_performance_schema = [
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("total_signals", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("paper_trades_executed", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("manual_trades_suggested", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("manual_trades_executed", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("paper_total_pnl", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("paper_win_rate", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("paper_avg_win", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("paper_avg_loss", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("paper_profit_factor", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("manual_total_pnl", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("manual_win_rate", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("paper_portfolio_value", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("manual_portfolio_value", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        # Portfolio Snapshots Table
        portfolio_snapshots_schema = [
            bigquery.SchemaField("snapshot_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("time", "TIME", mode="REQUIRED"),
            bigquery.SchemaField(
                "portfolio_type", "STRING", mode="REQUIRED"
            ),  # PAPER or MANUAL
            bigquery.SchemaField("total_value", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("cash_value", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("positions_value", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("day_pnl", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("total_pnl", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("active_positions", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("positions_detail", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        # Market Data Table
        market_data_schema = [
            bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("time", "TIME", mode="REQUIRED"),
            bigquery.SchemaField("open", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("high", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("low", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("close", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("volume", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField(
                "interval", "STRING", mode="REQUIRED"
            ),  # minute, day, etc.
            bigquery.SchemaField(
                "source", "STRING", mode="REQUIRED"
            ),  # kiteconnect, yahoo, etc.
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        # Create tables
        tables = {
            "trading_signals": signals_schema,
            "paper_trades": paper_trades_schema,
            "manual_trades": manual_trades_schema,
            "daily_performance": daily_performance_schema,
            "portfolio_snapshots": portfolio_snapshots_schema,
            "market_data": market_data_schema,
        }

        for table_name, schema in tables.items():
            self._create_table_if_not_exists(table_name, schema)

    def _create_table_if_not_exists(
        self, table_name: str, schema: list[bigquery.SchemaField]
    ):
        """Create BigQuery table if it doesn't exist."""
        try:
            table_ref = self.bq_client.dataset(self.dataset_id).table(table_name)

            try:
                self.bq_client.get_table(table_ref)
                logger.info("✅ Table {table_name} exists")
            except Exception:
                table = bigquery.Table(table_ref, schema=schema)

                # Set partitioning for large tables
                if table_name in [
                    "market_data",
                    "trading_signals",
                    "paper_trades",
                    "manual_trades",
                ]:
                    table.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY, field="created_at"
                    )

                self.bq_client.create_table(table)
                logger.info("✅ Created table {table_name}")

        except Exception as e:
            logger.error("❌ Failed to create table {table_name}: {e}")

    # BigQuery Insert Methods

    def insert_trading_signal(self, signal_data: dict[str, Any]) -> bool:
        """Insert trading signal into BigQuery."""
        try:
            table_ref = self.bq_client.dataset(self.dataset_id).table("trading_signals")

            # Add metadata
            signal_data["created_at"] = datetime.utcnow().isoformat()

            errors = self.bq_client.insert_rows_json(table_ref, [signal_data])

            if errors:
                logger.error("❌ Error inserting signal: {errors}")
                return False

            logger.info("✅ Signal inserted: {signal_data.get('symbol', 'Unknown')}")
            return True

        except Exception as e:
            logger.error("❌ Failed to insert signal: {e}")
            return False

    def insert_paper_trade(self, trade_data: dict[str, Any]) -> bool:
        """Insert paper trade into BigQuery."""
        try:
            table_ref = self.bq_client.dataset(self.dataset_id).table("paper_trades")

            # Add metadata
            trade_data["created_at"] = datetime.utcnow().isoformat()

            errors = self.bq_client.insert_rows_json(table_ref, [trade_data])

            if errors:
                logger.error("❌ Error inserting paper trade: {errors}")
                return False

            logger.info(
                "✅ Paper trade inserted: {trade_data.get('symbol', 'Unknown')}"
            )
            return True

        except Exception as e:
            logger.error("❌ Failed to insert paper trade: {e}")
            return False

    def insert_manual_trade(self, trade_data: dict[str, Any]) -> bool:
        """Insert manual trade into BigQuery."""
        try:
            table_ref = self.bq_client.dataset(self.dataset_id).table("manual_trades")

            # Add metadata
            trade_data["created_at"] = datetime.utcnow().isoformat()

            errors = self.bq_client.insert_rows_json(table_ref, [trade_data])

            if errors:
                logger.error("❌ Error inserting manual trade: {errors}")
                return False

            logger.info(
                "✅ Manual trade inserted: {trade_data.get('symbol', 'Unknown')}"
            )
            return True

        except Exception as e:
            logger.error("❌ Failed to insert manual trade: {e}")
            return False

    def insert_daily_performance(self, performance_data: dict[str, Any]) -> bool:
        """Insert daily performance into BigQuery."""
        try:
            table_ref = self.bq_client.dataset(self.dataset_id).table(
                "daily_performance"
            )

            # Add metadata
            performance_data["created_at"] = datetime.utcnow().isoformat()

            errors = self.bq_client.insert_rows_json(table_ref, [performance_data])

            if errors:
                logger.error("❌ Error inserting daily performance: {errors}")
                return False

            logger.info(
                "✅ Daily performance inserted: {performance_data.get('date', 'Unknown')}"
            )
            return True

        except Exception as e:
            logger.error("❌ Failed to insert daily performance: {e}")
            return False

    def insert_portfolio_snapshot(self, snapshot_data: dict[str, Any]) -> bool:
        """Insert portfolio snapshot into BigQuery."""
        try:
            table_ref = self.bq_client.dataset(self.dataset_id).table(
                "portfolio_snapshots"
            )

            # Add metadata
            snapshot_data["created_at"] = datetime.utcnow().isoformat()

            errors = self.bq_client.insert_rows_json(table_ref, [snapshot_data])

            if errors:
                logger.error("❌ Error inserting portfolio snapshot: {errors}")
                return False

            logger.info("✅ Portfolio snapshot inserted")
            return True

        except Exception as e:
            logger.error("❌ Failed to insert portfolio snapshot: {e}")
            return False

    def insert_market_data(self, market_data: list[dict[str, Any]]) -> bool:
        """Insert market data into BigQuery."""
        try:
            table_ref = self.bq_client.dataset(self.dataset_id).table("market_data")

            # Add metadata to each record
            for record in market_data:
                record["created_at"] = datetime.utcnow().isoformat()

            errors = self.bq_client.insert_rows_json(table_ref, market_data)

            if errors:
                logger.error("❌ Error inserting market data: {errors}")
                return False

            logger.info("✅ Market data inserted: {len(market_data)} records")
            return True

        except Exception as e:
            logger.error("❌ Failed to insert market data: {e}")
            return False

    # Firestore Methods (for real-time data)

    def save_live_signal_to_firestore(self, signal_data: dict[str, Any]) -> bool:
        """Save live signal to Firestore for real-time updates."""
        try:
            doc_ref = self.firestore_client.collection("live_signals").document(
                signal_data["signal_id"]
            )
            doc_ref.set(signal_data)

            logger.info(
                "✅ Live signal saved to Firestore: {signal_data.get('symbol', 'Unknown')}"
            )
            return True

        except Exception as e:
            logger.error("❌ Failed to save live signal: {e}")
            return False

    def save_portfolio_status_to_firestore(
        self, portfolio_data: dict[str, Any]
    ) -> bool:
        """Save current portfolio status to Firestore."""
        try:
            doc_ref = self.firestore_client.collection("portfolio_status").document(
                "current"
            )
            doc_ref.set(portfolio_data)

            logger.info("✅ Portfolio status saved to Firestore")
            return True

        except Exception as e:
            logger.error("❌ Failed to save portfolio status: {e}")
            return False

    # Query Methods

    def get_daily_performance(
        self, start_date: date, end_date: date
    ) -> list[dict[str, Any]]:
        """Get daily performance data from BigQuery."""
        try:
            query = """
            SELECT *
            FROM `{self.project_id}.{self.dataset_id}.daily_performance`
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY date DESC
            """

            query_job = self.bq_client.query(query)
            results = query_job.result()

            return [dict(row) for row in results]

        except Exception as e:
            logger.error("❌ Failed to get daily performance: {e}")
            return []

    def get_signal_performance(self, days: int = 30) -> list[dict[str, Any]]:
        """Get signal performance analysis."""
        try:
            query = """
            WITH signal_trades AS (
                SELECT
                    s.signal_id,
                    s.symbol,
                    s.signal_type,
                    s.confidence,
                    s.strategy_name,
                    p.pnl as paper_pnl,
                    p.pnl_percent as paper_pnl_percent,
                    m.pnl as manual_pnl,
                    m.pnl_percent as manual_pnl_percent
                FROM `{self.project_id}.{self.dataset_id}.trading_signals` s
                LEFT JOIN `{self.project_id}.{self.dataset_id}.paper_trades` p
                    ON s.signal_id = p.signal_id
                LEFT JOIN `{self.project_id}.{self.dataset_id}.manual_trades` m
                    ON s.signal_id = m.signal_id
                WHERE s.created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
            )
            SELECT
                strategy_name,
                confidence,
                COUNT(*) as total_signals,
                AVG(paper_pnl) as avg_paper_pnl,
                AVG(manual_pnl) as avg_manual_pnl,
                COUNTIF(paper_pnl > 0) / COUNT(paper_pnl) * 100 as paper_win_rate,
                COUNTIF(manual_pnl > 0) / COUNT(manual_pnl) * 100 as manual_win_rate
            FROM signal_trades
            GROUP BY strategy_name, confidence
            ORDER BY total_signals DESC
            """

            query_job = self.bq_client.query(query)
            results = query_job.result()

            return [dict(row) for row in results]

        except Exception as e:
            logger.error("❌ Failed to get signal performance: {e}")
            return []

    def cleanup_old_data(self, days_to_keep: int = 90):
        """Cleanup old data beyond retention period."""
        try:
            # Clean up old market data (keep only last 90 days)
            cleanup_query = """
            DELETE FROM `{self.project_id}.{self.dataset_id}.market_data`
            WHERE created_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_to_keep} DAY)
            """

            query_job = self.bq_client.query(cleanup_query)
            query_job.result()

            logger.info("✅ Cleaned up market data older than {days_to_keep} days")

        except Exception as e:
            logger.error("❌ Failed to cleanup old data: {e}")


# Example usage and setup script
def setup_database():
    """Setup script to initialize all database components."""
    print("🗄️  SETTING UP TRADING DATABASE")
    print("=" * 50)

    db_manager = DatabaseManager()

    print("📊 Initializing BigQuery...")
    db_manager.initialize_bigquery()

    print("🔥 Initializing Firestore...")
    db_manager.initialize_firestore()

    print("✅ Database setup complete!")
    print("\nDatabase Tables Created:")
    print("• trading_signals - All generated signals")
    print("• paper_trades - Automated simulation trades")
    print("• manual_trades - User-executed trades")
    print("• daily_performance - Daily aggregated metrics")
    print("• portfolio_snapshots - Portfolio value over time")
    print("• market_data - Historical price data")

    return db_manager


if __name__ == "__main__":
    setup_database()
