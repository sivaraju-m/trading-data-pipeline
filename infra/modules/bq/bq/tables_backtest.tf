# BigQuery Backtest Results Table
# ===============================

resource "google_bigquery_table" "backtest_results" {
  dataset_id          = google_bigquery_dataset.trading_data.dataset_id
  table_id            = "backtest_results"
  project             = var.project_id
  deletion_protection = false

  description = "Backtest results from AI Trading Machine strategies"

  time_partitioning {
    type                     = "DAY"
    field                    = "created_at"
  }

  clustering = ["strategy_id", "ticker", "success"]

  schema = jsonencode([
    {
      name = "backtest_id"
      type = "STRING"
      mode = "REQUIRED"
      description = "Unique identifier for the backtest run"
    },
    {
      name = "strategy_id"
      type = "STRING"
      mode = "REQUIRED"
      description = "Strategy name from registry"
    },
    {
      name = "ticker"
      type = "STRING"
      mode = "REQUIRED"
      description = "Stock ticker symbol"
    },
    {
      name = "start_date"
      type = "DATE"
      mode = "REQUIRED"
      description = "Backtest start date"
    },
    {
      name = "end_date"
      type = "DATE"
      mode = "REQUIRED"
      description = "Backtest end date"
    },
    {
      name = "initial_capital"
      type = "FLOAT"
      mode = "REQUIRED"
      description = "Starting capital amount"
    },
    {
      name = "final_value"
      type = "FLOAT"
      mode = "REQUIRED"
      description = "Final portfolio value"
    },
    {
      name = "total_return_pct"
      type = "FLOAT"
      mode = "REQUIRED"
      description = "Total return percentage"
    },
    {
      name = "sharpe_ratio"
      type = "FLOAT"
      mode = "NULLABLE"
      description = "Sharpe ratio of the strategy"
    },
    {
      name = "max_drawdown_pct"
      type = "FLOAT"
      mode = "NULLABLE"
      description = "Maximum drawdown percentage"
    },
    {
      name = "win_rate_pct"
      type = "FLOAT"
      mode = "NULLABLE"
      description = "Win rate percentage"
    },
    {
      name = "total_trades"
      type = "INTEGER"
      mode = "REQUIRED"
      description = "Total number of trades executed"
    },
    {
      name = "buy_hold_return_pct"
      type = "FLOAT"
      mode = "REQUIRED"
      description = "Buy and hold benchmark return"
    },
    {
      name = "outperformance_pct"
      type = "FLOAT"
      mode = "REQUIRED"
      description = "Outperformance vs buy and hold"
    },
    {
      name = "avg_confidence"
      type = "FLOAT"
      mode = "REQUIRED"
      description = "Average confidence score of signals"
    },
    {
      name = "data_points"
      type = "INTEGER"
      mode = "REQUIRED"
      description = "Number of data points used in backtest"
    },
    {
      name = "strategy_params"
      type = "JSON"
      mode = "NULLABLE"
      description = "Strategy parameters used"
    },
    {
      name = "transaction_costs"
      type = "FLOAT"
      mode = "NULLABLE"
      description = "Total transaction costs"
    },
    {
      name = "success"
      type = "BOOLEAN"
      mode = "REQUIRED"
      description = "Whether the backtest completed successfully"
    },
    {
      name = "error"
      type = "STRING"
      mode = "NULLABLE"
      description = "Error message if backtest failed"
    },
    {
      name = "created_at"
      type = "TIMESTAMP"
      mode = "REQUIRED"
      description = "Timestamp when backtest was created"
    }
  ])

  labels = {
    environment = var.environment
    service     = "ai-trading-machine"
    table_type  = "backtest_results"
  }
}

# Create view for successful backtests only
resource "google_bigquery_table" "successful_backtests" {
  dataset_id          = google_bigquery_dataset.trading_data.dataset_id
  table_id            = "successful_backtests"
  project             = var.project_id
  deletion_protection = false

  view {
    query = <<EOF
SELECT
  backtest_id,
  strategy_id,
  ticker,
  start_date,
  end_date,
  total_return_pct,
  sharpe_ratio,
  max_drawdown_pct,
  win_rate_pct,
  outperformance_pct,
  total_trades,
  avg_confidence,
  strategy_params,
  created_at
FROM `${var.project_id}.${google_bigquery_dataset.trading_data.dataset_id}.backtest_results`
WHERE success = TRUE
ORDER BY created_at DESC
EOF
    use_legacy_sql = false
  }
}
