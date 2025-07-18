output "workflows_service_account_email" {
  description = "Email of the workflows service account"
  value       = google_service_account.workflows_sa.email
}

output "workflow_names" {
  description = "Names of the created workflows"
  value = {
    backtest_run     = google_workflows_workflow.backtest_run.name
    history_pull     = google_workflows_workflow.history_pull.name
    model_train      = google_workflows_workflow.model_train.name
    signal_generate  = google_workflows_workflow.signal_generate.name
    token_refresh    = google_workflows_workflow.token_refresh.name
  }
}

output "workflow_execution_urls" {
  description = "URLs for executing workflows"
  value = {
    backtest_run = "https://workflowexecutions.googleapis.com/v1/projects/${var.project_id}/locations/${var.region}/workflows/backtest-run/executions"
    history_pull = "https://workflowexecutions.googleapis.com/v1/projects/${var.project_id}/locations/${var.region}/workflows/history-pull/executions"
    model_train  = "https://workflowexecutions.googleapis.com/v1/projects/${var.project_id}/locations/${var.region}/workflows/model-train/executions"
    signal_generate = "https://workflowexecutions.googleapis.com/v1/projects/${var.project_id}/locations/${var.region}/workflows/signal-generate/executions"
    token_refresh = "https://workflowexecutions.googleapis.com/v1/projects/${var.project_id}/locations/${var.region}/workflows/token-refresh/executions"
  }
}

output "scheduler_job_names" {
  description = "Names of the Cloud Scheduler jobs"
  value = {
    daily_data_pull       = google_cloud_scheduler_job.daily_data_pull.name
    token_refresh_schedule = google_cloud_scheduler_job.token_refresh_schedule.name
    signal_generation_schedule = google_cloud_scheduler_job.signal_generation_schedule.name
  }
}

output "bigquery_tables" {
  description = "BigQuery tables created for workflow logs"
  value = {
    workflow_logs        = google_bigquery_table.workflow_logs.table_id
    data_ingestion_log   = google_bigquery_table.data_ingestion_log.table_id
  }
}
