variable "project_id" {
  description = "GCP project ID"
  type        = string
  default     = "ai-trading-gcp-459813"
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "bucket_access" {
  description = "Map of GCS buckets to IAM roles and member bindings"
  type = map(object({
    role   = string
    member = string
  }))
  default = {
    "cleaned-data" = {
      role   = "roles/storage.objectViewer"
      member = "user:sivaraj.malladi@gmail.com"
    }
  }
}

variable "owner_email" {
  description = "Email address for resource ownership and BigQuery dataset access"
  type        = string
  default     = "sivaraj.malladi@gmail.com"
}

variable "admin_email" {
  description = "Email address for administrative access to BigQuery resources"
  type        = string
  default     = "sivaraj.malladi@gmail.com"
}

variable "bigquery_dataset_id" {
  description = "BigQuery dataset ID for workflows module"
  type        = string
  default     = "ai_trading_machine" # Updated to a valid dataset ID
}

variable "billing_account_id" {
  description = "Billing account ID for monitoring module"
  type        = string
  default     = "" # Ensure a valid ID is provided during deployment
}

variable "slack_webhook_url" {
  description = "Slack webhook URL for notifications"
  type        = string
  sensitive   = true
  default     = "" # Temporarily set to empty string to avoid errors
}
