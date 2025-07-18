# BigQuery Cost-Optimized Variables
#
# SJ-VERIFY
# - Path: /ai-trading-machine/infra/modules/bq_cost_optimized
# - Type: terraform
# - Checks: types,docs,sebi,gcp

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region for BigQuery dataset"
  type        = string
  default     = "us-central1" # Cheapest region
}

variable "dataset_id" {
  description = "BigQuery dataset ID"
  type        = string
  default     = "trading_data"
}

variable "environment" {
  description = "Environment (dev/staging/prod)"
  type        = string
  default     = "dev"
}

variable "admin_email" {
  description = "Admin email for BigQuery access"
  type        = string
}

# Cost optimization variables
variable "table_retention_days" {
  description = "Default table retention in days for cost optimization"
  type        = number
  default     = 1095 # 3 years for SEBI compliance

  validation {
    condition     = var.table_retention_days >= 30
    error_message = "Table retention must be at least 30 days."
  }
}

variable "partition_retention_days" {
  description = "Partition retention in days for cost optimization"
  type        = number
  default     = 90 # 3 months

  validation {
    condition     = var.partition_retention_days >= 1
    error_message = "Partition retention must be at least 1 day."
  }
}

variable "signals_retention_days" {
  description = "Signals table partition retention in days"
  type        = number
  default     = 365 # 1 year for signals
}

# Materialized views for cost optimization
variable "enable_materialized_views" {
  description = "Enable materialized views for cost optimization (disable for dev/test)"
  type        = bool
  default     = false # Disabled by default for cost savings
}

variable "mv_lookback_days" {
  description = "Materialized view lookback period in days"
  type        = number
  default     = 30
}

variable "mv_refresh_interval_hours" {
  description = "Materialized view refresh interval in hours"
  type        = number
  default     = 24 # Daily refresh
}

# Advanced cost optimization
variable "enable_query_cache" {
  description = "Enable query result caching for cost optimization"
  type        = bool
  default     = true
}

variable "max_bytes_billed" {
  description = "Maximum bytes billed per query (cost protection)"
  type        = number
  default     = 1073741824 # 1 GB limit
}

variable "job_timeout_ms" {
  description = "Job timeout in milliseconds (cost protection)"
  type        = number
  default     = 600000 # 10 minutes
}
