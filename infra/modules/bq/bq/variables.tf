variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "owner_email" {
  description = "Email address for BigQuery dataset owner"
  type        = string
}

variable "environment" {
  description = "Environment name (staging, production)"
  type        = string
}
