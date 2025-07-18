# Strategy Performance Tables
# ===========================

# Strategy performance tracking
resource "google_bigquery_table" "strategy_performance" {
  dataset_id          = google_bigquery_dataset.trading_data.dataset_id
  table_id            = "strategy_performance"
  deletion_protection = false

  description         = "Detailed strategy performance metrics and backtesting results"

  require_partition_filter = true

  time_partitioning {
    type  = "DAY"
    field = "backtest_date"
  }

  clustering = ["strategy_id", "symbol"]

  schema = jsonencode([
    { name = "strategy_id", type = "STRING", mode = "REQUIRED" },
    { name = "backtest_run_id", type = "STRING", mode = "REQUIRED" },
    { name = "symbol", type = "STRING", mode = "REQUIRED" },
    { name = "backtest_date", type = "DATE", mode = "REQUIRED" },
    { name = "start_date", type = "DATE", mode = "REQUIRED" },
    { name = "end_date", type = "DATE", mode = "REQUIRED" },
    { name = "total_return", type = "FLOAT64", mode = "NULLABLE" },
    { name = "annualized_return", type = "FLOAT64", mode = "NULLABLE" },
    { name = "sharpe_ratio", type = "FLOAT64", mode = "NULLABLE" },
    { name = "sortino_ratio", type = "FLOAT64", mode = "NULLABLE" },
    { name = "calmar_ratio", type = "FLOAT64", mode = "NULLABLE" },
    { name = "max_drawdown", type = "FLOAT64", mode = "NULLABLE" },
    { name = "volatility", type = "FLOAT64", mode = "NULLABLE" },
    { name = "win_rate", type = "FLOAT64", mode = "NULLABLE" },
    { name = "profit_factor", type = "FLOAT64", mode = "NULLABLE" },
    { name = "total_trades", type = "INTEGER", mode = "NULLABLE" },
    { name = "avg_trade_duration", type = "FLOAT64", mode = "NULLABLE" },
    { name = "largest_win", type = "FLOAT64", mode = "NULLABLE" },
    { name = "largest_loss", type = "FLOAT64", mode = "NULLABLE" },
    { name = "consecutive_wins", type = "INTEGER", mode = "NULLABLE" },
    { name = "consecutive_losses", type = "INTEGER", mode = "NULLABLE" },
    { name = "parameters", type = "JSON", mode = "NULLABLE" },
    { name = "created_at", type = "TIMESTAMP", mode = "REQUIRED" }
  ])

}

# Strategy validation summary (enhanced existing table)
resource "google_bigquery_table" "strategy_validation_summary" {
  dataset_id          = google_bigquery_dataset.trading_data.dataset_id
  table_id            = "strategy_validation_summary"
  deletion_protection = false

  description         = "High-level strategy validation summary for quick analysis"

  require_partition_filter = true

  time_partitioning {
    type  = "DAY"
    field = "validation_date"
  }

  clustering = ["strategy", "time_period"]

  schema = jsonencode([
    { name = "strategy", type = "STRING", mode = "REQUIRED" },
    { name = "time_period", type = "STRING", mode = "REQUIRED" },
    { name = "validation_date", type = "DATE", mode = "REQUIRED" },
    { name = "num_tickers", type = "INTEGER", mode = "REQUIRED" },
    { name = "success_rate", type = "FLOAT", mode = "NULLABLE" },
    { name = "avg_cagr", type = "FLOAT", mode = "NULLABLE" },
    { name = "avg_sharpe", type = "FLOAT", mode = "NULLABLE" },
    { name = "max_drawdown", type = "FLOAT", mode = "NULLABLE" },
    { name = "win_rate", type = "FLOAT", mode = "NULLABLE" },
    { name = "total_backtest_runs", type = "INTEGER", mode = "NULLABLE" },
    { name = "confidence_score", type = "FLOAT", mode = "NULLABLE" },
    { name = "created_at", type = "TIMESTAMP", mode = "REQUIRED" }
  ])

}
