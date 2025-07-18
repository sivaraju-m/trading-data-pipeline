# External Tables for Cost Optimization
# Point to data stored in Cloud Storage for cheaper access
# NOTE: Temporarily disabled until GCS files are available

# Union view combining hot data only (simplified without external tables for now)
resource "google_bigquery_table" "ohlcv_unified_view" {
  dataset_id          = google_bigquery_dataset.trading_data.dataset_id
  table_id            = "ohlcv_unified_view"
  deletion_protection = false

  description = "Unified view for OHLCV data (currently only hot data from BigQuery)"

  view {
    query          = <<EOF
-- Hot data from BigQuery
SELECT
  symbol, date, timestamp, open, high, low, close, volume,
  adjusted_close, sector, market_cap, data_source,
  'hot' as data_tier
FROM `${var.project_id}.trading_data.ohlcv_data`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
EOF
    use_legacy_sql = false
  }
}

# Performance monitoring view for tables (simplified)
resource "google_bigquery_table" "table_metrics_view" {
  dataset_id          = google_bigquery_dataset.trading_data.dataset_id
  table_id            = "table_metrics_view"
  deletion_protection = false

  description = "Monitor performance and usage of tables"

  view {
    query          = <<EOF
SELECT
  'ohlcv_data' as table_name,
  COUNT(*) as row_count,
  'BigQuery Native Table' as table_type,
  CURRENT_TIMESTAMP() as last_checked
FROM `${var.project_id}.trading_data.ohlcv_data`
WHERE DATE(created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
EOF
    use_legacy_sql = false
  }
}
