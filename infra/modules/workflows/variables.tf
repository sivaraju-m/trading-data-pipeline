variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "region" {
  description = "The GCP region"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "production"
}

variable "bigquery_dataset_id" {
  description = "BigQuery dataset ID for storing workflow logs"
  type        = string
}

variable "default_symbols" {
  description = "Default symbols for data ingestion"
  type        = list(string)
  default     = ["RELIANCE.NS", "TCS.NS", "HDFC.NS", "INFY.NS", "ICICIBANK.NS"]
}

variable "default_model_id" {
  description = "Default model ID for signal generation"
  type        = string
  default     = "default_model"
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}
