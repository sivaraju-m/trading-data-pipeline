# BigQuery Cost-Optimized Module
#
# SJ-VERIFY
# - Path: /ai-trading-machine/infra/modules/bq_cost_optimized
# - Type: terraform
# - Checks: types,docs,sebi,gcp
#
# Purpose: Cost-optimized BigQuery configuration for trading data

terraform {
  required_version = ">= 1.3"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

# Use existing dataset from main bigquery module
data "google_bigquery_dataset" "trading_data" {
  dataset_id = var.dataset_id
  project    = var.project_id
}

# Partitioned and clustered table for price data
resource "google_bigquery_table" "price_data" {
  dataset_id = data.google_bigquery_dataset.trading_data.dataset_id
  table_id   = "price_data"

  description = "Partitioned price data table for cost optimization"

  # Partition by date for query cost optimization
  time_partitioning {
    type          = "DAY"
    field         = "date"
    expiration_ms = var.partition_retention_days * 24 * 60 * 60 * 1000
  }

  require_partition_filter = true # Force partition filtering

  # Cluster by frequently queried fields
  clustering = ["ticker", "exchange"]

  schema = jsonencode([
    {
      name        = "date"
      type        = "DATE"
      description = "Trading date"
    },
    {
      name        = "ticker"
      type        = "STRING"
      description = "Stock ticker symbol"
    },
    {
      name        = "exchange"
      type        = "STRING"
      description = "Exchange name (NSE/BSE)"
    },
    {
      name        = "open"
      type        = "FLOAT64"
      description = "Opening price"
    },
    {
      name        = "high"
      type        = "FLOAT64"
      description = "Highest price"
    },
    {
      name        = "low"
      type        = "FLOAT64"
      description = "Lowest price"
    },
    {
      name        = "close"
      type        = "FLOAT64"
      description = "Closing price"
    },
    {
      name        = "volume"
      type        = "INT64"
      description = "Trading volume"
    },
  ])

  labels = {
    optimized  = "true"
    table_type = "price_data"
  }

  deletion_protection = true
}

# Signals table with optimized structure
resource "google_bigquery_table" "signals" {
  dataset_id = data.google_bigquery_dataset.trading_data.dataset_id
  table_id   = "signals"

  description = "Trading signals with cost-optimized partitioning"

  time_partitioning {
    type          = "DAY"
    field         = "timestamp"
    expiration_ms = var.signals_retention_days * 24 * 60 * 60 * 1000
  }

  require_partition_filter = true

  clustering = ["ticker", "action", "strategy"]

  schema = jsonencode([
    {
      name = "signal_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "timestamp"
      type = "TIMESTAMP"
      mode = "REQUIRED"
    },
    {
      name = "ticker"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "action"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "confidence"
      type = "FLOAT64"
      mode = "REQUIRED"
    },
    {
      name = "strategy"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "price"
      type = "FLOAT64"
      mode = "NULLABLE"
    },
  ])

  labels = {
    optimized  = "true"
    table_type = "signals"
  }

  deletion_protection = true
}

# Materialized view for common queries (optional for cost savings)
resource "google_bigquery_table" "daily_signals_mv" {
  count = var.enable_materialized_views ? 1 : 0

  dataset_id = data.google_bigquery_dataset.trading_data.dataset_id
  table_id   = "daily_signals_mv"

  materialized_view {
    query = <<EOF
SELECT
  DATE(timestamp) as date,
  ticker,
  COUNT(*) as signal_count,
  AVG(confidence) as avg_confidence,
  COUNTIF(action = 'BUY') as buy_signals,
  COUNTIF(action = 'SELL') as sell_signals,
  COUNTIF(action = 'HOLD') as hold_signals
FROM `${var.project_id}.${data.google_bigquery_dataset.trading_data.dataset_id}.signals`
WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL ${var.mv_lookback_days} DAY)
GROUP BY date, ticker
EOF

    enable_refresh      = true
    refresh_interval_ms = var.mv_refresh_interval_hours * 60 * 60 * 1000
  }
}

# Cost optimization view for query analysis
resource "google_bigquery_table" "cost_optimization_view" {
  dataset_id = data.google_bigquery_dataset.trading_data.dataset_id
  table_id   = "cost_optimization_view"

  view {
    query          = <<EOF
SELECT
  'price_data' as table_name,
  COUNT(*) as row_count,
  ROUND(COUNT(*) * 0.001, 2) as estimated_mb -- Simplified size estimation
FROM `${var.project_id}.${data.google_bigquery_dataset.trading_data.dataset_id}.price_data`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)

UNION ALL

SELECT
  'signals' as table_name,
  COUNT(*) as row_count,
  ROUND(COUNT(*) * 0.001, 2) as estimated_mb -- Simplified size estimation
FROM `${var.project_id}.${data.google_bigquery_dataset.trading_data.dataset_id}.signals`
WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
EOF
    use_legacy_sql = false
  }
}

output "price_data_table_id" {
  description = "The ID of the price data table"
  value       = google_bigquery_table.price_data.table_id
}
