# BigQuery Module Outputs

# Dataset information
output "dataset_id" {
  description = "The ID of the BigQuery dataset"
  value       = google_bigquery_dataset.trading_data.dataset_id
}

output "dataset_location" {
  description = "The location of the BigQuery dataset"
  value       = google_bigquery_dataset.trading_data.location
}

# Core tables
output "ohlcv_table_id" {
  description = "The ID of the OHLCV data table"
  value       = google_bigquery_table.ohlcv_data.table_id
}

output "technical_indicators_table_id" {
  description = "The ID of the technical indicators table"
  value       = google_bigquery_table.technical_indicators.table_id
}

output "trades_execution_table_id" {
  description = "The ID of the trades execution table"
  value       = google_bigquery_table.trades_execution.table_id
}

output "portfolio_positions_table_id" {
  description = "The ID of the portfolio positions table"
  value       = google_bigquery_table.portfolio_positions.table_id
}

output "strategy_performance_table_id" {
  description = "The ID of the strategy performance table"
  value       = google_bigquery_table.strategy_performance.table_id
}

output "strategy_validation_summary_table_id" {
  description = "The ID of the strategy validation summary table"
  value       = google_bigquery_table.strategy_validation_summary.table_id
}

# Materialized views
output "daily_returns_mv_id" {
  description = "The ID of the daily returns materialized view"
  value       = google_bigquery_table.daily_returns_mv.table_id
}

output "strategy_metrics_mv_id" {
  description = "The ID of the strategy metrics materialized view"
  value       = google_bigquery_table.strategy_metrics_mv.table_id
}

output "portfolio_summary_mv_id" {
  description = "The ID of the portfolio summary materialized view"
  value       = google_bigquery_table.portfolio_summary_mv.table_id
}

output "risk_metrics_mv_id" {
  description = "The ID of the risk metrics materialized view"
  value       = google_bigquery_table.risk_metrics_mv.table_id
}

# Views
output "ohlcv_unified_view_id" {
  description = "The ID of the unified OHLCV view"
  value       = google_bigquery_table.ohlcv_unified_view.table_id
}

# Table references for use in other modules
output "table_references" {
  description = "Full table references for use in queries"
  value = {
    ohlcv_data           = "${var.project_id}.${google_bigquery_dataset.trading_data.dataset_id}.${google_bigquery_table.ohlcv_data.table_id}"
    technical_indicators = "${var.project_id}.${google_bigquery_dataset.trading_data.dataset_id}.${google_bigquery_table.technical_indicators.table_id}"
    trades_execution     = "${var.project_id}.${google_bigquery_dataset.trading_data.dataset_id}.${google_bigquery_table.trades_execution.table_id}"
    portfolio_positions  = "${var.project_id}.${google_bigquery_dataset.trading_data.dataset_id}.${google_bigquery_table.portfolio_positions.table_id}"
    strategy_performance = "${var.project_id}.${google_bigquery_dataset.trading_data.dataset_id}.${google_bigquery_table.strategy_performance.table_id}"
    daily_returns_mv     = "${var.project_id}.${google_bigquery_dataset.trading_data.dataset_id}.${google_bigquery_table.daily_returns_mv.table_id}"
    strategy_metrics_mv  = "${var.project_id}.${google_bigquery_dataset.trading_data.dataset_id}.${google_bigquery_table.strategy_metrics_mv.table_id}"
    portfolio_summary_mv = "${var.project_id}.${google_bigquery_dataset.trading_data.dataset_id}.${google_bigquery_table.portfolio_summary_mv.table_id}"
    risk_metrics_mv      = "${var.project_id}.${google_bigquery_dataset.trading_data.dataset_id}.${google_bigquery_table.risk_metrics_mv.table_id}"
    ohlcv_unified_view   = "${var.project_id}.${google_bigquery_dataset.trading_data.dataset_id}.${google_bigquery_table.ohlcv_unified_view.table_id}"
  }
}

# Cost optimization information
output "cost_optimization_info" {
  description = "Information about cost optimization features"
  value = {
    partitioned_tables = [
      google_bigquery_table.ohlcv_data.table_id,
      google_bigquery_table.technical_indicators.table_id,
      google_bigquery_table.trades_execution.table_id,
      google_bigquery_table.portfolio_positions.table_id,
      google_bigquery_table.strategy_performance.table_id,
      google_bigquery_table.strategy_validation_summary.table_id
    ]
    clustered_tables = [
      google_bigquery_table.ohlcv_data.table_id,
      google_bigquery_table.technical_indicators.table_id,
      google_bigquery_table.trades_execution.table_id,
      google_bigquery_table.portfolio_positions.table_id,
      google_bigquery_table.strategy_performance.table_id
    ]
    materialized_views = [
      google_bigquery_table.daily_returns_mv.table_id,
      google_bigquery_table.strategy_metrics_mv.table_id,
      google_bigquery_table.portfolio_summary_mv.table_id,
      google_bigquery_table.risk_metrics_mv.table_id
    ]
    external_tables = [
      google_bigquery_table.ohlcv_unified_view.table_id,
      google_bigquery_table.table_metrics_view.table_id
    ]
    data_retention_days = 2555 # 7 years for SEBI compliance
  }
}

# Grouped outputs for main module reference
output "table_ids" {
  description = "Map of all BigQuery table IDs"
  value = {
    ohlcv_data                    = google_bigquery_table.ohlcv_data.table_id
    technical_indicators          = google_bigquery_table.technical_indicators.table_id
    trades_execution             = google_bigquery_table.trades_execution.table_id
    portfolio_positions          = google_bigquery_table.portfolio_positions.table_id
    strategy_performance         = google_bigquery_table.strategy_performance.table_id
    strategy_validation_summary  = google_bigquery_table.strategy_validation_summary.table_id
  }
}

output "materialized_view_ids" {
  description = "Map of materialized view IDs"
  value = {
    daily_returns_mv     = google_bigquery_table.daily_returns_mv.table_id
    strategy_metrics_mv  = google_bigquery_table.strategy_metrics_mv.table_id
    portfolio_summary_mv = google_bigquery_table.portfolio_summary_mv.table_id
    risk_metrics_mv      = google_bigquery_table.risk_metrics_mv.table_id
  }
}

output "external_table_ids" {
  description = "Map of external table IDs"
  value = {
    ohlcv_unified_view = google_bigquery_table.ohlcv_unified_view.table_id
    table_metrics_view = google_bigquery_table.table_metrics_view.table_id
  }
}
