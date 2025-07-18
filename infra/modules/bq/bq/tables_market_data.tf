# Core Market Data Tables
# =======================

# Core OHLCV data table - optimized for time-series queries
resource "google_bigquery_table" "ohlcv_data" {
  dataset_id          = google_bigquery_dataset.trading_data.dataset_id
  table_id            = "ohlcv_data"
  deletion_protection = false

  description = "Historical and real-time OHLCV data with technical indicators"

  require_partition_filter = true

  time_partitioning {
    type  = "DAY"
    field = "date"
  }

  clustering = ["symbol", "sector"]

  schema = jsonencode([
    { name = "symbol", type = "STRING", mode = "REQUIRED" },
    { name = "date", type = "DATE", mode = "REQUIRED" },
    { name = "timestamp", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "open", type = "FLOAT64", mode = "REQUIRED" },
    { name = "high", type = "FLOAT64", mode = "REQUIRED" },
    { name = "low", type = "FLOAT64", mode = "REQUIRED" },
    { name = "close", type = "FLOAT64", mode = "REQUIRED" },
    { name = "volume", type = "INTEGER", mode = "REQUIRED" },
    { name = "adjusted_close", type = "FLOAT64", mode = "NULLABLE" },
    { name = "sector", type = "STRING", mode = "NULLABLE" },
    { name = "market_cap", type = "STRING", mode = "NULLABLE" },
    { name = "data_source", type = "STRING", mode = "REQUIRED" },
    { name = "data_quality_score", type = "FLOAT64", mode = "NULLABLE" },
    { name = "created_at", type = "TIMESTAMP", mode = "REQUIRED" }
  ])

}

# Technical indicators table - pre-computed for performance
resource "google_bigquery_table" "technical_indicators" {
  dataset_id          = google_bigquery_dataset.trading_data.dataset_id
  table_id            = "technical_indicators"
  deletion_protection = false

  description = "Pre-computed technical indicators for all symbols"

  require_partition_filter = true

  time_partitioning {
    type  = "DAY"
    field = "date"
  }

  clustering = ["symbol", "indicator_type"]

  schema = jsonencode([
    { name = "symbol", type = "STRING", mode = "REQUIRED" },
    { name = "date", type = "DATE", mode = "REQUIRED" },
    { name = "indicator_type", type = "STRING", mode = "REQUIRED" },
    { name = "rsi_14", type = "FLOAT64", mode = "NULLABLE" },
    { name = "rsi_21", type = "FLOAT64", mode = "NULLABLE" },
    { name = "ema_20", type = "FLOAT64", mode = "NULLABLE" },
    { name = "ema_50", type = "FLOAT64", mode = "NULLABLE" },
    { name = "ema_200", type = "FLOAT64", mode = "NULLABLE" },
    { name = "macd_line", type = "FLOAT64", mode = "NULLABLE" },
    { name = "macd_signal", type = "FLOAT64", mode = "NULLABLE" },
    { name = "macd_histogram", type = "FLOAT64", mode = "NULLABLE" },
    { name = "bb_upper", type = "FLOAT64", mode = "NULLABLE" },
    { name = "bb_middle", type = "FLOAT64", mode = "NULLABLE" },
    { name = "bb_lower", type = "FLOAT64", mode = "NULLABLE" },
    { name = "stoch_k", type = "FLOAT64", mode = "NULLABLE" },
    { name = "stoch_d", type = "FLOAT64", mode = "NULLABLE" },
    { name = "atr", type = "FLOAT64", mode = "NULLABLE" },
    { name = "adx", type = "FLOAT64", mode = "NULLABLE" },
    { name = "created_at", type = "TIMESTAMP", mode = "REQUIRED" }
  ])

}
