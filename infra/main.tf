# Simple Terraform Configuration for GitHub Actions
# Licensed by SJ Trading
# ==================================================

terraform {
  required_version = ">= 1.3"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Enable required APIs
resource "google_project_service" "required_apis" {
  for_each = toset([
    "run.googleapis.com",
    "bigquery.googleapis.com",
    "firestore.googleapis.com",
    "secretmanager.googleapis.com",
    "monitoring.googleapis.com",
    "logging.googleapis.com",
    "cloudscheduler.googleapis.com",
    "containerregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "workflows.googleapis.com",
    "aiplatform.googleapis.com",
    "artifactregistry.googleapis.com"
  ])

  project = var.project_id
  service = each.key

  disable_on_destroy = false
}

# GitHub Actions Service Account Module
module "github_actions" {
  source = "./modules/github_actions"

  project_id        = var.project_id
  region            = var.region
  github_repository = "sivarajumalladi/ai-trading-machine"

  depends_on = [google_project_service.required_apis]
}

# BigQuery Data Warehouse Module
module "bigquery" {
  source = "./modules/bq"

  project_id  = var.project_id
  environment = var.environment
  region      = var.region
  owner_email = var.owner_email

  depends_on = [google_project_service.required_apis]
}

# BigQuery Cost Optimized Module
module "bq_cost_optimized" {
  source = "./modules/bq_cost_optimized"

  project_id  = var.project_id
  environment = var.environment
  region      = var.region
  admin_email = var.admin_email

  depends_on = [module.bigquery]
}

# Cloud Run for Backtest API
module "backtest_api" {
  source = "./modules/cloudrun"

  project_id            = var.project_id
  region                = var.region
  service_name          = "ai-trading-backtest-api"
  image                 = "gcr.io/${var.project_id}/ai-trading-backtest-api:latest"
  service_account_email = module.github_actions.service_account_email

  depends_on = [google_project_service.required_apis]
}

# Firestore for Configuration Storage
module "firestore" {
  source = "./modules/firestore"

  project_id = var.project_id
  region     = var.region

  depends_on = [google_project_service.required_apis]
}

# PubSub for Async Processing
module "pubsub" {
  source        = "./modules/pubsub"
  project_id    = var.project_id
  subscriptions = []
  topics        = []

  depends_on = [google_project_service.required_apis]
}

# Cloud Workflows for Orchestration
module "workflows" {
  source = "./modules/workflows"

  project_id          = var.project_id
  region              = var.region
  environment         = var.environment
  bigquery_dataset_id = var.bigquery_dataset_id

  depends_on = [google_project_service.required_apis]
}

# Monitoring and Cost Control
module "monitoring" {
  source = "./modules/monitoring"

  project_id         = var.project_id
  region             = var.region
  environment        = var.environment
  billing_account_id = var.billing_account_id
  admin_email        = var.admin_email
  slack_webhook_url  = var.slack_webhook_url

  depends_on = [google_project_service.required_apis]
}
