# Trading Execution Tables
# ========================

# Trade execution logs - SEBI compliance
resource "google_bigquery_table" "trades_execution" {
  dataset_id          = google_bigquery_dataset.trading_data.dataset_id
  table_id            = "trades_execution"
  deletion_protection = false

  description         = "Trade execution logs for SEBI compliance and performance tracking"

  require_partition_filter = true

  time_partitioning {
    type  = "DAY"
    field = "trade_date"
  }

  clustering = ["strategy_id", "symbol"]

  schema = jsonencode([
    { name = "trade_id", type = "STRING", mode = "REQUIRED" },
    { name = "strategy_id", type = "STRING", mode = "REQUIRED" },
    { name = "symbol", type = "STRING", mode = "REQUIRED" },
    { name = "trade_date", type = "DATE", mode = "REQUIRED" },
    { name = "trade_timestamp", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "trade_type", type = "STRING", mode = "REQUIRED" },
    { name = "quantity", type = "INTEGER", mode = "REQUIRED" },
    { name = "price", type = "FLOAT64", mode = "REQUIRED" },
    { name = "total_value", type = "FLOAT64", mode = "REQUIRED" },
    { name = "fees", type = "FLOAT64", mode = "NULLABLE" },
    { name = "taxes", type = "FLOAT64", mode = "NULLABLE" },
    { name = "order_id", type = "STRING", mode = "NULLABLE" },
    { name = "execution_venue", type = "STRING", mode = "NULLABLE" },
    { name = "signal_source", type = "STRING", mode = "NULLABLE" },
    { name = "portfolio_id", type = "STRING", mode = "NULLABLE" },
    { name = "user_id", type = "STRING", mode = "NULLABLE" },
    { name = "created_at", type = "TIMESTAMP", mode = "REQUIRED" }
  ])

}
