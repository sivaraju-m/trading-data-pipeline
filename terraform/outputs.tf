# Trading Data Pipeline - Terraform Outputs
# Key information for CI/CD and monitoring

output "cloud_run_url" {
  description = "URL of the deployed Cloud Run service"
  value       = google_cloud_run_v2_service.main.uri
}

output "service_account_email" {
  description = "Email of the service account used by Cloud Run"
  value       = google_service_account.cloud_run.email
}

output "image_name_tag" {
  description = "Full image name with tag for deployment"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.main.repository_id}/${var.image_name}:${var.image_tag}"
}

output "artifact_registry_repository" {
  description = "Artifact Registry repository name"
  value       = google_artifact_registry_repository.main.name
}

output "bigquery_dataset_id" {
  description = "BigQuery dataset ID for trading data"
  value       = google_bigquery_dataset.trading_data.dataset_id
}

output "bigquery_table_id" {
  description = "BigQuery table ID for market data"
  value       = "${google_bigquery_dataset.trading_data.dataset_id}.${google_bigquery_table.market_data.table_id}"
}

output "scheduler_job_name" {
  description = "Cloud Scheduler job name"
  value       = google_cloud_scheduler_job.daily_ingestion.name
}

output "monitoring_uptime_check_id" {
  description = "Uptime check monitoring ID"
  value       = google_monitoring_uptime_check_config.service_uptime.uptime_check_id
}

output "deployment_info" {
  description = "Complete deployment information"
  value = {
    service_url          = google_cloud_run_v2_service.main.uri
    service_name         = var.service_name
    region              = var.region
    project_id          = var.project_id
    environment         = var.environment
    image_registry      = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.main.repository_id}"
    service_account     = google_service_account.cloud_run.email
    bigquery_dataset    = google_bigquery_dataset.trading_data.dataset_id
    scheduler_schedule  = "30 18 * * 1-5"
    health_check_url    = "${google_cloud_run_v2_service.main.uri}/health"
  }
}
