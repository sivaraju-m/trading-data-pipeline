# Looker Studio Integration for AI Trading Machine
# ================================================

# Note: Looker Studio connections must be created manually through the UI
# This file provides the necessary BigQuery data source configuration
# and sample dashboard configuration for automation

# Data Source Configuration for Looker Studio
# Copy this configuration when setting up Looker Studio data source

locals {
  looker_config = {
    project_id = var.project_id
    dataset_id = google_bigquery_dataset.trading_data.dataset_id

    # Main tables for dashboard
    tables = {
      backtest_results = google_bigquery_table.backtest_results.table_id
      successful_backtests = google_bigquery_table.successful_backtests.table_id
      trading_signals = "trading_signals"  # From other module
      paper_trades = "paper_trades"        # From other module
    }

    # Recommended charts and metrics
    dashboard_components = {
      # Chart 1: Sharpe Ratio vs Strategy
      sharpe_by_strategy = {
        chart_type = "column"
        dimension = "strategy_id"
        metric = "AVG(sharpe_ratio)"
        filter = "success = TRUE"
      }

      # Chart 2: Drawdown vs Parameters Heatmap
      drawdown_heatmap = {
        chart_type = "heatmap"
        dimensions = ["strategy_id", "ticker"]
        metric = "AVG(max_drawdown_pct)"
        filter = "success = TRUE AND max_drawdown_pct IS NOT NULL"
      }

      # Chart 3: Win Rate Analysis
      win_rate_analysis = {
        chart_type = "scatter"
        x_axis = "total_trades"
        y_axis = "win_rate_pct"
        size = "total_return_pct"
        color = "strategy_id"
      }

      # Chart 4: Performance Timeline
      performance_timeline = {
        chart_type = "time_series"
        date_dimension = "created_at"
        metric = "total_return_pct"
        breakdown = "strategy_id"
      }

      # Chart 5: Outperformance Distribution
      outperformance_dist = {
        chart_type = "histogram"
        dimension = "outperformance_pct"
        filter = "success = TRUE"
      }
    }
  }
}

# Create a view specifically optimized for Looker Studio
resource "google_bigquery_table" "looker_dashboard_data" {
  dataset_id          = google_bigquery_dataset.trading_data.dataset_id
  table_id            = "looker_dashboard_data"
  project             = var.project_id
  deletion_protection = false

  view {
    query = <<EOF
WITH strategy_performance AS (
  SELECT
    strategy_id,
    ticker,
    DATE(created_at) as backtest_date,
    created_at,
    total_return_pct,
    sharpe_ratio,
    max_drawdown_pct,
    win_rate_pct,
    outperformance_pct,
    total_trades,
    avg_confidence,
    EXTRACT(YEAR FROM created_at) as backtest_year,
    EXTRACT(MONTH FROM created_at) as backtest_month,
    -- Performance categories
    CASE
      WHEN total_return_pct > 15 THEN 'High Performer'
      WHEN total_return_pct > 5 THEN 'Medium Performer'
      WHEN total_return_pct > 0 THEN 'Low Performer'
      ELSE 'Underperformer'
    END as performance_category,
    -- Risk categories
    CASE
      WHEN max_drawdown_pct < 5 THEN 'Low Risk'
      WHEN max_drawdown_pct < 15 THEN 'Medium Risk'
      ELSE 'High Risk'
    END as risk_category,
    -- Strategy parameters (extract common ones)
    JSON_EXTRACT_SCALAR(strategy_params, '$.period') as period_param,
    JSON_EXTRACT_SCALAR(strategy_params, '$.lookback') as lookback_param,
    JSON_EXTRACT_SCALAR(strategy_params, '$.adaptive_thresholds') as adaptive_param
  FROM `${var.project_id}.${google_bigquery_dataset.trading_data.dataset_id}.backtest_results`
  WHERE success = TRUE
),
aggregated_metrics AS (
  SELECT
    strategy_id,
    COUNT(*) as total_backtests,
    AVG(total_return_pct) as avg_return,
    STDDEV(total_return_pct) as return_volatility,
    AVG(sharpe_ratio) as avg_sharpe,
    AVG(max_drawdown_pct) as avg_drawdown,
    AVG(win_rate_pct) as avg_win_rate,
    AVG(outperformance_pct) as avg_outperformance,
    COUNT(DISTINCT ticker) as tickers_tested,
    MAX(backtest_date) as last_backtest_date
  FROM strategy_performance
  GROUP BY strategy_id
)
SELECT
  sp.*,
  am.total_backtests,
  am.avg_return as strategy_avg_return,
  am.return_volatility,
  am.avg_sharpe as strategy_avg_sharpe,
  am.avg_drawdown as strategy_avg_drawdown,
  am.avg_win_rate as strategy_avg_win_rate,
  am.avg_outperformance as strategy_avg_outperformance,
  am.tickers_tested,
  am.last_backtest_date
FROM strategy_performance sp
LEFT JOIN aggregated_metrics am ON sp.strategy_id = am.strategy_id      ORDER BY sp.backtest_date DESC
EOF
    use_legacy_sql = false
  }

  description = "Optimized view for Looker Studio dashboards with pre-calculated metrics"

  labels = {
    environment = var.environment
    service     = "ai-trading-machine"
    table_type  = "looker_view"
  }
}

# Output the Looker Studio configuration
output "looker_studio_config" {
  description = "Configuration for setting up Looker Studio dashboard"
  value = {
    project_id = var.project_id
    dataset_id = google_bigquery_dataset.trading_data.dataset_id
    main_table = google_bigquery_table.looker_dashboard_data.table_id
    data_source_url = "https://datastudio.google.com/datasources/create?connectorId=bigQuery&projectId=${var.project_id}&datasetId=${google_bigquery_dataset.trading_data.dataset_id}&tableId=${google_bigquery_table.looker_dashboard_data.table_id}"
    dashboard_components = local.looker_config.dashboard_components
  }
}

# Sample SQL queries for manual dashboard creation
output "sample_looker_queries" {
  description = "Sample SQL queries for Looker Studio dashboard creation"
  value = {
    strategy_performance = "SELECT strategy_id, AVG(total_return_pct) as avg_return, AVG(sharpe_ratio) as avg_sharpe FROM ${google_bigquery_table.looker_dashboard_data.table_id} GROUP BY strategy_id ORDER BY avg_return DESC"

    risk_return_scatter = "SELECT total_return_pct, max_drawdown_pct, strategy_id, ticker FROM ${google_bigquery_table.looker_dashboard_data.table_id} WHERE max_drawdown_pct IS NOT NULL"

    performance_timeline = "SELECT backtest_date, strategy_id, AVG(total_return_pct) as daily_avg_return FROM ${google_bigquery_table.looker_dashboard_data.table_id} GROUP BY backtest_date, strategy_id ORDER BY backtest_date"

    win_rate_heatmap = "SELECT strategy_id, ticker, AVG(win_rate_pct) as avg_win_rate FROM ${google_bigquery_table.looker_dashboard_data.table_id} WHERE win_rate_pct IS NOT NULL GROUP BY strategy_id, ticker"
  }
}
