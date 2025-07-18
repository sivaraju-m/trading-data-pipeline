terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

# Enable required APIs
resource "google_project_service" "workflows_api" {
  project = var.project_id
  service = "workflows.googleapis.com"

  disable_dependent_services = true
  disable_on_destroy         = false
}

resource "google_project_service" "workflowexecutions_api" {
  project = var.project_id
  service = "workflowexecutions.googleapis.com"

  disable_dependent_services = true
  disable_on_destroy         = false
}

# Service account for workflows
resource "google_service_account" "workflows_sa" {
  account_id   = "ai-trading-workflows"
  display_name = "AI Trading Workflows Service Account"
  description  = "Service account for executing Cloud Workflows"

  depends_on = [google_project_service.workflows_api]
}

# IAM bindings for the service account
resource "google_project_iam_member" "workflows_sa_bigquery" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.workflows_sa.email}"
}

resource "google_project_iam_member" "workflows_sa_storage" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.workflows_sa.email}"
}

resource "google_project_iam_member" "workflows_sa_cloudrun" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.workflows_sa.email}"
}

resource "google_project_iam_member" "workflows_sa_monitoring" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.workflows_sa.email}"
}

resource "google_project_iam_member" "workflows_sa_logging" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.workflows_sa.email}"
}

# Backtest workflow
resource "google_workflows_workflow" "backtest_run" {
  name               = "backtest-run"
  region             = var.region
  description        = "Workflow for running backtests"
  service_account    = google_service_account.workflows_sa.email
  deletion_protection = false

  source_contents = file("${path.module}/../../../workflows/backtest_run.yaml")

  depends_on = [
    google_project_service.workflows_api,
    google_service_account.workflows_sa
  ]

  labels = {
    component = "ai-trading"
    type      = "backtest"
    env       = var.environment
  }
}

# History pull workflow
resource "google_workflows_workflow" "history_pull" {
  name               = "history-pull"
  region             = var.region
  description        = "Workflow for pulling historical data"
  service_account    = google_service_account.workflows_sa.email
  deletion_protection = false

  source_contents = file("${path.module}/../../../workflows/history_pull.yaml")

  depends_on = [
    google_project_service.workflows_api,
    google_service_account.workflows_sa
  ]

  labels = {
    component = "ai-trading"
    type      = "data-ingestion"
    env       = var.environment
  }
}

# Model training workflow
resource "google_workflows_workflow" "model_train" {
  name               = "model-train"
  region             = var.region
  description        = "Workflow for training ML models"
  service_account    = google_service_account.workflows_sa.email
  deletion_protection = false

  source_contents = file("${path.module}/../../../workflows/model_train.yaml")

  depends_on = [
    google_project_service.workflows_api,
    google_service_account.workflows_sa
  ]

  labels = {
    component = "ai-trading"
    type      = "ml-training"
    env       = var.environment
  }
}

# Signal generation workflow
resource "google_workflows_workflow" "signal_generate" {
  name               = "signal-generate"
  region             = var.region
  description        = "Workflow for generating trading signals"
  service_account    = google_service_account.workflows_sa.email
  deletion_protection = false

  source_contents = file("${path.module}/../../../workflows/signal_generate.yaml")

  depends_on = [
    google_project_service.workflows_api,
    google_service_account.workflows_sa
  ]

  labels = {
    component = "ai-trading"
    type      = "signal-generation"
    env       = var.environment
  }
}

# Token refresh workflow
resource "google_workflows_workflow" "token_refresh" {
  name               = "token-refresh"
  region             = var.region
  description        = "Workflow for refreshing API tokens"
  service_account    = google_service_account.workflows_sa.email
  deletion_protection = false

  source_contents = file("${path.module}/../../../workflows/token_refresh.yaml")

  depends_on = [
    google_project_service.workflows_api,
    google_service_account.workflows_sa
  ]

  labels = {
    component = "ai-trading"
    type      = "token-management"
    env       = var.environment
  }
}

# Cloud Scheduler jobs to trigger workflows
resource "google_cloud_scheduler_job" "daily_data_pull" {
  name      = "daily-data-pull"
  region    = var.region
  schedule  = "0 9 * * 1-5"  # 9 AM on weekdays
  time_zone = "Asia/Kolkata"

  http_target {
    http_method = "POST"
    uri         = "https://workflowexecutions.googleapis.com/v1/projects/${var.project_id}/locations/${var.region}/workflows/history-pull/executions"

    headers = {
      "Content-Type" = "application/json"
    }

    body = base64encode(jsonencode({
      argument = jsonencode({
        symbols = var.default_symbols
        source  = "yahoo"
      })
    }))

    oauth_token {
      service_account_email = google_service_account.workflows_sa.email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }
}

resource "google_cloud_scheduler_job" "token_refresh_schedule" {
  name      = "token-refresh-schedule"
  region    = var.region
  schedule  = "0 */4 * * *"  # Every 4 hours
  time_zone = "Asia/Kolkata"

  http_target {
    http_method = "POST"
    uri         = "https://workflowexecutions.googleapis.com/v1/projects/${var.project_id}/locations/${var.region}/workflows/token-refresh/executions"

    headers = {
      "Content-Type" = "application/json"
    }

    body = base64encode(jsonencode({
      argument = jsonencode({})
    }))

    oauth_token {
      service_account_email = google_service_account.workflows_sa.email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }
}

resource "google_cloud_scheduler_job" "signal_generation_schedule" {
  name      = "signal-generation-schedule"
  region    = var.region
  schedule  = "*/15 9-15 * * 1-5"  # Every 15 minutes during market hours
  time_zone = "Asia/Kolkata"

  http_target {
    http_method = "POST"
    uri         = "https://workflowexecutions.googleapis.com/v1/projects/${var.project_id}/locations/${var.region}/workflows/signal-generate/executions"

    headers = {
      "Content-Type" = "application/json"
    }

    body = base64encode(jsonencode({
      argument = jsonencode({
        model_id = var.default_model_id
        symbols  = var.default_symbols
        confidence_threshold = 0.7
      })
    }))

    oauth_token {
      service_account_email = google_service_account.workflows_sa.email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }
}

# BigQuery dataset for staging
resource "google_bigquery_dataset" "staging" {
  dataset_id                  = "staging"
  project                     = "ai-trading-gcp-459813"
  location                    = "asia-south1"
  default_table_expiration_ms = 7776000000
  description                 = "Staging dataset for workflows"
}

# BigQuery tables for workflow logs
resource "google_bigquery_table" "workflow_logs" {
  dataset_id          = var.bigquery_dataset_id
  table_id            = "workflow_logs"
  deletion_protection = false

  schema = jsonencode([
    {
      name = "workflow_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "step_name"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "status"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "error_message"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "timestamp"
      type = "TIMESTAMP"
      mode = "REQUIRED"
    }
  ])

  time_partitioning {
    type  = "DAY"
    field = "timestamp"
  }

  clustering = ["workflow_id", "status"]
}

resource "google_bigquery_table" "data_ingestion_log" {
  dataset_id          = var.bigquery_dataset_id
  table_id            = "data_ingestion_log"
  deletion_protection = false

  schema = jsonencode([
    {
      name = "job_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "symbols"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "start_date"
      type = "DATE"
      mode = "REQUIRED"
    },
    {
      name = "end_date"
      type = "DATE"
      mode = "REQUIRED"
    },
    {
      name = "records_count"
      type = "INTEGER"
      mode = "REQUIRED"
    },
    {
      name = "quality_score"
      type = "FLOAT"
      mode = "REQUIRED"
    },
    {
      name = "source"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "created_at"
      type = "TIMESTAMP"
      mode = "REQUIRED"
    },
    {
      name = "workflow_execution_id"
      type = "STRING"
      mode = "NULLABLE"
    }
  ])

  time_partitioning {
    type  = "DAY"
    field = "created_at"
  }

  clustering = ["job_id", "source"]
}

resource "google_bigquery_table" "model_registry" {
  dataset_id          = var.bigquery_dataset_id
  table_id            = "model_registry"
  deletion_protection = false

  schema = jsonencode([
    {
      name = "model_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "model_type"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "features"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "target"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "r2_score"
      type = "FLOAT"
      mode = "REQUIRED"
    },
    {
      name = "mse"
      type = "FLOAT"
      mode = "REQUIRED"
    },
    {
      name = "mae"
      type = "FLOAT"
      mode = "REQUIRED"
    },
    {
      name = "version"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "artifacts_path"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "created_at"
      type = "TIMESTAMP"
      mode = "REQUIRED"
    },
    {
      name = "status"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "workflow_execution_id"
      type = "STRING"
      mode = "NULLABLE"
    }
  ])

  time_partitioning {
    type  = "DAY"
    field = "created_at"
  }

  clustering = ["model_id", "status"]
}

resource "google_bigquery_table" "trading_signals" {
  dataset_id          = var.bigquery_dataset_id
  table_id            = "trading_signals"
  deletion_protection = false

  schema = jsonencode([
    {
      name = "signal_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "model_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "symbol"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "signal_type"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "confidence"
      type = "FLOAT"
      mode = "REQUIRED"
    },
    {
      name = "position_size"
      type = "FLOAT"
      mode = "REQUIRED"
    },
    {
      name = "target_price"
      type = "FLOAT"
      mode = "NULLABLE"
    },
    {
      name = "stop_loss"
      type = "FLOAT"
      mode = "NULLABLE"
    },
    {
      name = "created_at"
      type = "TIMESTAMP"
      mode = "REQUIRED"
    },
    {
      name = "status"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "workflow_execution_id"
      type = "STRING"
      mode = "NULLABLE"
    }
  ])

  time_partitioning {
    type  = "DAY"
    field = "created_at"
  }

  clustering = ["signal_id", "model_id", "symbol"]
}

resource "google_bigquery_table" "token_refresh_log" {
  dataset_id          = var.bigquery_dataset_id
  table_id            = "token_refresh_log"
  deletion_protection = false

  schema = jsonencode([
    {
      name = "refresh_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "tokens_refreshed"
      type = "INTEGER"
      mode = "REQUIRED"
    },
    {
      name = "successful_refreshes"
      type = "INTEGER"
      mode = "REQUIRED"
    },
    {
      name = "failed_refreshes"
      type = "INTEGER"
      mode = "REQUIRED"
    },
    {
      name = "created_at"
      type = "TIMESTAMP"
      mode = "REQUIRED"
    },
    {
      name = "workflow_execution_id"
      type = "STRING"
      mode = "NULLABLE"
    }
  ])

  time_partitioning {
    type  = "DAY"
    field = "created_at"
  }

  clustering = ["refresh_id"]
}
