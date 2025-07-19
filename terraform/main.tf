# Trading Data Pipeline - Terraform Main Configuration
# Optimized for production deployment with security and cost efficiency

terraform {
  required_version = ">= 1.5"
  
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
  
  # Uncomment for remote state management
  # backend "gcs" {
  #   bucket = "your-terraform-state-bucket"
  #   prefix = "trading-data-pipeline"
  # }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Enable required GCP APIs
resource "google_project_service" "apis" {
  for_each = toset([
    "run.googleapis.com",
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "bigquery.googleapis.com",
    "cloudscheduler.googleapis.com",
    "secretmanager.googleapis.com",
    "monitoring.googleapis.com",
    "logging.googleapis.com"
  ])
  
  project = var.project_id
  service = each.key
  
  disable_on_destroy = false
}

# Artifact Registry for Docker images
resource "google_artifact_registry_repository" "main" {
  location      = var.region
  repository_id = "${var.service_name}-repo"
  description   = "Docker repository for ${var.service_name}"
  format        = "DOCKER"
  
  depends_on = [google_project_service.apis]
}

# Service Account for Cloud Run (least privilege)
resource "google_service_account" "cloud_run" {
  account_id   = "${var.service_name}-runner"
  display_name = "Cloud Run Service Account for ${var.service_name}"
  description  = "Service account for ${var.service_name} with minimal required permissions"
}

# IAM bindings for service account (least privilege principle)
resource "google_project_iam_member" "cloud_run_permissions" {
  for_each = toset([
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/secretmanager.secretAccessor",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter"
  ])
  
  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.cloud_run.email}"
}

# BigQuery dataset for trading data
resource "google_bigquery_dataset" "trading_data" {
  dataset_id    = "trading_data_${var.environment}"
  friendly_name = "Trading Data - ${title(var.environment)}"
  description   = "Dataset for trading market data and analytics"
  location      = "US"
  
  # Enable deletion protection in production
  delete_contents_on_destroy = var.environment != "prod"
  
  depends_on = [google_project_service.apis]
}

# BigQuery table for market data (partitioned and clustered)
resource "google_bigquery_table" "market_data" {
  dataset_id = google_bigquery_dataset.trading_data.dataset_id
  table_id   = "market_data"
  
  description = "OHLCV market data with partitioning and clustering"
  
  time_partitioning {
    type  = "DAY"
    field = "date"
  }
  
  clustering = ["symbol", "sector"]
  
  schema = jsonencode([
    {
      name = "symbol"
      type = "STRING"
      mode = "REQUIRED"
      description = "Stock symbol"
    },
    {
      name = "date"
      type = "DATE"
      mode = "REQUIRED"
      description = "Trading date"
    },
    {
      name = "open"
      type = "FLOAT64"
      mode = "REQUIRED"
      description = "Opening price"
    },
    {
      name = "high"
      type = "FLOAT64"
      mode = "REQUIRED"
      description = "High price"
    },
    {
      name = "low"
      type = "FLOAT64"
      mode = "REQUIRED"
      description = "Low price"
    },
    {
      name = "close"
      type = "FLOAT64"
      mode = "REQUIRED"
      description = "Closing price"
    },
    {
      name = "volume"
      type = "INT64"
      mode = "REQUIRED"
      description = "Trading volume"
    },
    {
      name = "sector"
      type = "STRING"
      mode = "NULLABLE"
      description = "Stock sector"
    },
    {
      name = "timestamp"
      type = "TIMESTAMP"
      mode = "REQUIRED"
      description = "Ingestion timestamp"
    }
  ])
}

# Cloud Run service with optimized configuration
resource "google_cloud_run_v2_service" "main" {
  name     = var.service_name
  location = var.region
  
  template {
    service_account = google_service_account.cloud_run.email
    
    # Cost optimization settings
    scaling {
      min_instance_count = var.min_instances
      max_instance_count = var.max_instances
    }
    
    containers {
      image = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.main.repository_id}/${var.image_name}:${var.image_tag}"
      
      # Resource limits for cost efficiency
      resources {
        limits = {
          cpu    = var.cpu_limit
          memory = var.memory_limit
        }
        startup_cpu_boost = true
      }
      
      # Health check port
      ports {
        container_port = 8080
      }
      
      # Environment variables
      env {
        name  = "ENVIRONMENT"
        value = var.environment
      }
      
      env {
        name  = "PROJECT_ID"
        value = var.project_id
      }
      
      env {
        name  = "BIGQUERY_DATASET"
        value = google_bigquery_dataset.trading_data.dataset_id
      }
    }
    
    # Request timeout
    timeout = "${var.timeout_seconds}s"
    
    # Maximum concurrent requests per instance
    max_instance_request_concurrency = var.max_concurrency
  }
  
  traffic {
    percent = 100
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
  }
  
  depends_on = [
    google_project_service.apis,
    google_project_iam_member.cloud_run_permissions
  ]
}

# Allow unauthenticated access (adjust for production needs)
resource "google_cloud_run_service_iam_member" "public_access" {
  location = google_cloud_run_v2_service.main.location
  service  = google_cloud_run_v2_service.main.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

# Cloud Scheduler job for daily data ingestion
resource "google_cloud_scheduler_job" "daily_ingestion" {
  name        = "${var.service_name}-daily"
  description = "Daily data ingestion job"
  schedule    = "30 18 * * 1-5"  # 6:30 PM IST, Monday-Friday
  time_zone   = "Asia/Kolkata"
  
  http_target {
    http_method = "POST"
    uri         = "${google_cloud_run_v2_service.main.uri}/trigger-daily-ingestion"
    
    headers = {
      "Content-Type" = "application/json"
    }
    
    body = base64encode(jsonencode({
      source = "scheduler"
      date   = "auto"
    }))
    
    oidc_token {
      service_account_email = google_service_account.cloud_run.email
    }
  }
  
  retry_config {
    retry_count = 3
  }
  
  depends_on = [google_project_service.apis]
}

# Monitoring: Uptime check
resource "google_monitoring_uptime_check_config" "service_uptime" {
  display_name = "${var.service_name} Uptime Check"
  timeout      = "10s"
  period       = "300s"
  
  http_check {
    path         = "/health"
    port         = "443"
    use_ssl      = true
    validate_ssl = true
  }
  
  monitored_resource {
    type = "uptime_url"
    labels = {
      host       = split("://", google_cloud_run_v2_service.main.uri)[1]
      project_id = var.project_id
    }
  }
}

# Budget alert (optional cost control)
resource "google_billing_budget" "service_budget" {
  count = var.environment == "prod" ? 1 : 0
  
  billing_account = var.billing_account_id
  display_name    = "${var.service_name} Budget"
  
  budget_filter {
    projects = ["projects/${var.project_id}"]
    services = [
      "services/run.googleapis.com",
      "services/bigquery.googleapis.com"
    ]
  }
  
  amount {
    specified_amount {
      currency_code = "USD"
      units         = "100"  # $100 monthly budget
    }
  }
  
  threshold_rules {
    threshold_percent = 0.8
    spend_basis       = "CURRENT_SPEND"
  }
  
  threshold_rules {
    threshold_percent = 1.0
    spend_basis       = "CURRENT_SPEND"
  }
}
