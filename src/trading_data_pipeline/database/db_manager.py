#!/usr/bin/env python3
"""
Database Configuration and Management
====================================
Manages separate databases for testing and production modes.
"""

import json
import logging
import sqlite3
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any


class DatabaseMode(Enum):
    TESTING = "testing"
    PRODUCTION = "production"


class DatabaseManager:
    """Manages database connections and operations for testing and production"""

    def __init__(self, mode: DatabaseMode = DatabaseMode.TESTING):
        self.mode = mode
        self.config = self._load_database_config()
        self.setup_logging()
        self._ensure_databases_exist()

    def _load_database_config(self) -> dict[str, Any]:
        """Load database configuration"""
        config = {
            "testing": {
                "source_db": "data/testing/source.db",
                "results_db": "data/testing/results.db",
                "analytics_db": "data/testing/analytics.db",
                "max_connections": 5,
                "enable_wal": True,
            },
            "production": {
                "source_db": "data/production/source.db",
                "results_db": "data/production/results.db",
                "analytics_db": "data/production/analytics.db",
                "max_connections": 20,
                "enable_wal": True,
            },
        }

        # Load custom config if available
        config_file = Path("config/database_config.json")
        if config_file.exists():
            with open(config_file) as f:
                custom_config = json.load(f)
                for mode_name in config:
                    if mode_name in custom_config:
                        config[mode_name].update(custom_config[mode_name])

        return config

    def setup_logging(self):
        """Setup database logging"""
        self.logger = logging.getLogger("db_manager_{self.mode.value}")

    def _ensure_databases_exist(self):
        """Ensure all required databases exist"""
        mode_config = self.config[self.mode.value]

        for db_type, db_path in mode_config.items():
            if db_type.endswith("_db"):
                full_path = Path(db_path)
                full_path.parent.mkdir(parents=True, exist_ok=True)

                if not full_path.exists():
                    self._create_database(full_path, db_type)
                    self.logger.info("✅ Created {db_type}: {full_path}")

    def _create_database(self, db_path: Path, db_type: str):
        """Create a new database with appropriate schema"""
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        try:
            if db_type == "source_db":
                self._create_source_schema(cursor)
            elif db_type == "results_db":
                self._create_results_schema(cursor)
            elif db_type == "analytics_db":
                self._create_analytics_schema(cursor)

            conn.commit()
            self.logger.info("📊 Created schema for {db_type}")

        except Exception as e:
            self.logger.error("❌ Error creating {db_type}: {e}")
            conn.rollback()
        finally:
            conn.close()

    def _create_source_schema(self, cursor):
        """Create source database schema"""
        # Market data table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS market_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                timestamp DATETIME NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume INTEGER NOT NULL,
                source TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, timestamp, source)
            )
        """
        )

        # Signals table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id TEXT UNIQUE NOT NULL,
                symbol TEXT NOT NULL,
                strategy TEXT NOT NULL,
                action TEXT NOT NULL,
                confidence REAL NOT NULL,
                timestamp DATETIME NOT NULL,
                metadata TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Data sync tracking
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS sync_tracking (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                last_sync DATETIME NOT NULL,
                sync_type TEXT NOT NULL,
                status TEXT NOT NULL,
                records_count INTEGER DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Create indexes
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_market_data_symbol_timestamp ON market_data(symbol, timestamp)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_signals_symbol_timestamp ON signals(symbol, timestamp)"
        )

    def _create_results_schema(self, cursor):
        """Create results database schema"""
        # Backtest results
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS backtest_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                backtest_id TEXT UNIQUE NOT NULL,
                strategy TEXT NOT NULL,
                symbol TEXT NOT NULL,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                initial_capital REAL NOT NULL,
                final_value REAL NOT NULL,
                total_return_pct REAL NOT NULL,
                sharpe_ratio REAL,
                max_drawdown_pct REAL,
                trades_count INTEGER NOT NULL,
                win_rate_pct REAL,
                metadata TEXT,
                mode TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Paper trades
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id TEXT UNIQUE NOT NULL,
                strategy TEXT NOT NULL,
                symbol TEXT NOT NULL,
                action TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                price REAL,
                timestamp DATETIME NOT NULL,
                status TEXT NOT NULL,
                pnl REAL,
                mode TEXT NOT NULL,
                metadata TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Performance metrics
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS performance_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metrics_id TEXT UNIQUE NOT NULL,
                period_start DATE NOT NULL,
                period_end DATE NOT NULL,
                strategy TEXT,
                symbol TEXT,
                metric_type TEXT NOT NULL,
                metric_value REAL NOT NULL,
                mode TEXT NOT NULL,
                metadata TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Create indexes
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_backtest_strategy_symbol ON backtest_results(strategy, symbol)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_paper_trades_symbol_timestamp ON paper_trades(symbol, timestamp)"
        )

    def _create_analytics_schema(self, cursor):
        """Create analytics database schema"""
        # Analytics reports
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS analytics_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                report_id TEXT UNIQUE NOT NULL,
                report_type TEXT NOT NULL,
                period_start DATE NOT NULL,
                period_end DATE NOT NULL,
                summary TEXT NOT NULL,
                details TEXT,
                mode TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Risk metrics
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS risk_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metric_id TEXT UNIQUE NOT NULL,
                symbol TEXT,
                strategy TEXT,
                risk_type TEXT NOT NULL,
                risk_value REAL NOT NULL,
                threshold_value REAL,
                status TEXT NOT NULL,
                timestamp DATETIME NOT NULL,
                mode TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Compliance audits
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS compliance_audits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                audit_id TEXT UNIQUE NOT NULL,
                audit_type TEXT NOT NULL,
                status TEXT NOT NULL,
                violations_count INTEGER DEFAULT 0,
                details TEXT,
                auditor TEXT,
                mode TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

    def get_connection(self, db_type: str = "source_db"):
        """Get database connection for specified type"""
        mode_config = self.config[self.mode.value]
        db_path = mode_config.get(db_type)

        if not db_path:
            raise ValueError(
                "Database type {db_type} not configured for {self.mode.value}"
            )

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # Enable column access by name

        # Enable WAL mode for better concurrency
        if mode_config.get("enable_wal", False):
            conn.execute("PRAGMA journal_mode=WAL")

        return conn

    def save_market_data(
        self, symbol: str, data: list[dict[str, Any]], source: str = "yfinance"
    ):
        """Save market data to source database"""
        conn = self.get_connection("source_db")
        cursor = conn.cursor()

        try:
            for record in data:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO market_data
                    (symbol, timestamp, open, high, low, close, volume, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        symbol,
                        record["timestamp"],
                        record["open"],
                        record["high"],
                        record["low"],
                        record["close"],
                        record["volume"],
                        source,
                    ),
                )

            conn.commit()
            self.logger.info("💾 Saved {len(data)} records for {symbol} from {source}")

        except Exception as e:
            self.logger.error("❌ Error saving market data: {e}")
            conn.rollback()
        finally:
            conn.close()

    def save_backtest_result(self, result: dict[str, Any]):
        """Save backtest result to results database"""
        conn = self.get_connection("results_db")
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                INSERT OR REPLACE INTO backtest_results
                (backtest_id, strategy, symbol, start_date, end_date, initial_capital,
                 final_value, total_return_pct, sharpe_ratio, max_drawdown_pct,
                 trades_count, win_rate_pct, metadata, mode)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    result.get(
                        "backtest_id", "BT_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    ),
                    result["strategy"],
                    result["symbol"],
                    result.get("start_date"),
                    result.get("end_date"),
                    result.get("initial_capital", 100000),
                    result.get("final_value", 0),
                    result.get("total_return_pct", 0),
                    result.get("sharpe_ratio"),
                    result.get("max_drawdown_pct"),
                    result.get("trades", 0),
                    result.get("win_rate", 0),
                    json.dumps(result.get("metadata", {})),
                    self.mode.value,
                ),
            )

            conn.commit()
            self.logger.info(
                "💾 Saved backtest result for {result['strategy']} on {result['symbol']}"
            )

        except Exception as e:
            self.logger.error("❌ Error saving backtest result: {e}")
            conn.rollback()
        finally:
            conn.close()

    def get_recent_data(self, symbol: str, hours: int = 24) -> list[dict[str, Any]]:
        """Get recent market data for a symbol"""
        conn = self.get_connection("source_db")
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT * FROM market_data
            WHERE symbol = ? AND timestamp >= datetime('now', '-{} hours')
            ORDER BY timestamp DESC
        """.format(
                hours
            ),
            (symbol,),
        )

        results = [dict(row) for row in cursor.fetchall()]
        conn.close()

        return results

    def get_database_stats(self) -> dict[str, Any]:
        """Get database statistics for current mode"""
        stats = {
            "mode": self.mode.value,
            "timestamp": datetime.now().isoformat(),
            "databases": {},
        }

        mode_config = self.config[self.mode.value]

        for db_type, db_path in mode_config.items():
            if db_type.endswith("_db"):
                try:
                    conn = self.get_connection(db_type)
                    cursor = conn.cursor()

                    # Get table info
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                    tables = [row[0] for row in cursor.fetchall()]

                    table_stats = {}
                    for table in tables:
                        cursor.execute("SELECT COUNT(*) FROM {table}")
                        count = cursor.fetchone()[0]
                        table_stats[table] = count

                    stats["databases"][db_type] = {
                        "path": db_path,
                        "tables": table_stats,
                        "total_records": sum(table_stats.values()),
                    }

                    conn.close()

                except Exception as e:
                    stats["databases"][db_type] = {"error": str(e)}

        return stats


def create_test_data(mode: DatabaseMode = DatabaseMode.TESTING):
    """Create minimal test data for testing database"""
    db_manager = DatabaseManager(mode)

    # Create test market data
    test_symbols = ["RELIANCE.NS", "TCS.NS", "INFY.NS"]
    base_time = datetime.now()

    for i, symbol in enumerate(test_symbols):
        test_data = []
        for j in range(10):  # 10 data points per symbol
            timestamp = base_time.replace(
                hour=9 + j, minute=15, second=0, microsecond=0
            )
            test_data.append(
                {
                    "timestamp": timestamp.isoformat(),
                    "open": 100 + i * 10 + j,
                    "high": 105 + i * 10 + j,
                    "low": 95 + i * 10 + j,
                    "close": 102 + i * 10 + j,
                    "volume": 1000 * (j + 1),
                }
            )

        db_manager.save_market_data(symbol, test_data, "test_data")

    # Create test backtest results
    for symbol in test_symbols:
        test_result = {
            "strategy": "test_rsi",
            "symbol": symbol,
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "total_return_pct": 5.5,
            "trades": 10,
            "win_rate": 60,
            "metadata": {"test": True},
        }
        db_manager.save_backtest_result(test_result)

    print("✅ Created test data for {mode.value} database")
    stats = db_manager.get_database_stats()
    print("📊 Database stats: {json.dumps(stats, indent=2)}")


def main():
    """Demo database management"""
    # Test both modes
    for mode in [DatabaseMode.TESTING, DatabaseMode.PRODUCTION]:
        print("\n🗄️ Testing {mode.value} database...")

        # Create database manager
        db_manager = DatabaseManager(mode)

        # Get stats
        stats = db_manager.get_database_stats()
        print("📊 {mode.value} stats: {json.dumps(stats, indent=2)}")

    # Create test data for testing mode
    print("\n🧪 Creating test data...")
    create_test_data(DatabaseMode.TESTING)


if __name__ == "__main__":
    main()
