# Portfolio Management Tables
# ===========================

# Portfolio positions - current holdings
resource "google_bigquery_table" "portfolio_positions" {
  dataset_id          = google_bigquery_dataset.trading_data.dataset_id
  table_id            = "portfolio_positions"
  deletion_protection = false

  description         = "Current portfolio positions and historical snapshots"

  require_partition_filter = true

  time_partitioning {
    type  = "DAY"
    field = "position_date"
  }

  clustering = ["portfolio_id", "symbol"]

  schema = jsonencode([
    { name = "portfolio_id", type = "STRING", mode = "REQUIRED" },
    { name = "symbol", type = "STRING", mode = "REQUIRED" },
    { name = "position_date", type = "DATE", mode = "REQUIRED" },
    { name = "quantity", type = "INTEGER", mode = "REQUIRED" },
    { name = "avg_price", type = "FLOAT64", mode = "REQUIRED" },
    { name = "current_price", type = "FLOAT64", mode = "REQUIRED" },
    { name = "market_value", type = "FLOAT64", mode = "REQUIRED" },
    { name = "unrealized_pnl", type = "FLOAT64", mode = "REQUIRED" },
    { name = "realized_pnl", type = "FLOAT64", mode = "REQUIRED" },
    { name = "total_invested", type = "FLOAT64", mode = "REQUIRED" },
    { name = "sector", type = "STRING", mode = "NULLABLE" },
    { name = "strategy_id", type = "STRING", mode = "NULLABLE" },
    { name = "allocation_percentage", type = "FLOAT64", mode = "NULLABLE" },
    { name = "risk_score", type = "FLOAT64", mode = "NULLABLE" },
    { name = "last_trade_date", type = "DATE", mode = "NULLABLE" },
    { name = "created_at", type = "TIMESTAMP", mode = "REQUIRED" }
  ])

}
