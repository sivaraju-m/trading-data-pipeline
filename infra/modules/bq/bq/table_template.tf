# BigQuery Table Creation Template
# ================================
# Use this template for creating new BigQuery tables
# Follow the standardized pattern for consistency

# TEMPLATE: CREATE TABLE <category>_<table_name>
# ==============================================

/*
resource "google_bigquery_table" "<category>_<table_name>" {
  dataset_id = google_bigquery_dataset.trading_data.dataset_id
  table_id   = "<category>_<table_name>"
  deletion_protection = false  # Set to true for production

  description = "<Brief description of the table purpose>"

  # STANDARD PARTITIONING (Cost Optimization)
  time_partitioning {
    type  = "DAY"                    # Always use DAY partitioning
    field = "<date_field>"           # Usually: date, trade_date, created_date
    require_partition_filter = true  # Force partition filtering for cost control
  }

  # STANDARD CLUSTERING (Query Performance)
  clustering = ["<high_cardinality_field1>", "<high_cardinality_field2>"]
  # Common patterns:
  # - Market data: ["symbol", "sector"]
  # - Trading: ["strategy_id", "symbol"]
  # - Portfolio: ["portfolio_id", "symbol"]
  # - Strategy: ["strategy_id", "symbol"]

  # SCHEMA DEFINITION
  schema = jsonencode([
    # REQUIRED FIELDS (Business Keys)
    { name = "<primary_key>", type = "STRING", mode = "REQUIRED" },
    { name = "<date_field>", type = "DATE", mode = "REQUIRED" },

    # BUSINESS DATA FIELDS
    { name = "<field_name>", type = "<data_type>", mode = "<REQUIRED|NULLABLE>" },

    # STANDARD AUDIT FIELDS (Always include)
    { name = "created_at", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "updated_at", type = "TIMESTAMP", mode = "NULLABLE" }
  ])

  # STANDARD RETENTION (SEBI Compliance)
}
*/

# QUICK REFERENCE: Common Data Types
# ==================================
# STRING     - Text data, IDs, names
# INTEGER    - Whole numbers, quantities
# FLOAT64    - Decimal numbers, prices, ratios
# DATE       - Date only (YYYY-MM-DD)
# TIMESTAMP  - Date and time with timezone
# BOOLEAN    - True/false values
# JSON       - Complex nested data structures
# ARRAY      - Lists of values
# STRUCT     - Nested record structures

# QUICK REFERENCE: Field Modes
# =============================
# REQUIRED   - Field must have a value
# NULLABLE   - Field can be null/empty
# REPEATED   - Field can have multiple values (array)

# QUICK REFERENCE: Partitioning Fields
# ====================================
# Market Data:    date, timestamp
# Trading:        trade_date, execution_date
# Portfolio:      position_date, snapshot_date
# Strategy:       backtest_date, validation_date
# Signals:        signal_date, generated_date

# QUICK REFERENCE: Clustering Fields
# ==================================
# Always use high-cardinality fields that are frequently filtered
# Order by most selective to least selective
# Common patterns:
# - symbol (most selective in trading data)
# - strategy_id (selective for strategy-specific queries)
# - portfolio_id (selective for portfolio queries)
# - sector (medium selectivity)
# - signal_type (medium selectivity)

# EXAMPLES: Table Creation Commands
# =================================

# Example 1: Market Data Table
# CREATE TABLE market_ohlcv_data
# Partitioned by: date
# Clustered by: symbol, sector

# Example 2: Trading Execution Table
# CREATE TABLE trading_executions
# Partitioned by: trade_date
# Clustered by: strategy_id, symbol

# Example 3: Portfolio Positions Table
# CREATE TABLE portfolio_positions
# Partitioned by: position_date
# Clustered by: portfolio_id, symbol

# Example 4: Strategy Performance Table
# CREATE TABLE strategy_performance
# Partitioned by: backtest_date
# Clustered by: strategy_id, symbol
