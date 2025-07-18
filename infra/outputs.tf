# AI Trading Machine - Infrastructure Outputs
# ============================================

# BigQuery Module Outputs
output "bigquery_dataset_id" {
  description = "BigQuery dataset ID for trading data"
  value       = module.bigquery.dataset_id
}

output "bigquery_tables" {
  description = "Map of all BigQuery tables with their full IDs"
  value       = module.bigquery.table_ids
}

output "bigquery_materialized_views" {
  description = "Map of materialized views for high-performance queries"
  value       = module.bigquery.materialized_view_ids
}

output "bigquery_external_tables" {
  description = "Map of external tables for cost-optimized storage"
  value       = module.bigquery.external_table_ids
}

# GitHub Actions Module Outputs
output "github_actions_sa_email" {
  description = "Service account email for GitHub Actions"
  value       = module.github_actions.service_account_email
}

# Quick Reference Commands
output "quick_reference" {
  description = "Quick reference commands for BigQuery tables"
  value = {
    create_market_tables    = "terraform apply -target=module.bigquery.google_bigquery_table.market_ohlcv_data"
    create_trading_tables   = "terraform apply -target=module.bigquery.google_bigquery_table.trading_executions"
    create_portfolio_tables = "terraform apply -target=module.bigquery.google_bigquery_table.portfolio_positions"
    create_strategy_tables  = "terraform apply -target=module.bigquery.google_bigquery_table.strategy_performance"
    view_all_tables         = "terraform state list | grep bigquery_table"
  }
}
