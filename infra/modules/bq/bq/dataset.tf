# BigQuery Dataset Configuration
# ===============================

resource "google_bigquery_dataset" "trading_data" {
  dataset_id                 = "trading_data"
  location                   = var.region
  delete_contents_on_destroy = true

  description = "AI Trading Machine - Core trading data warehouse"

  access {
    role          = "OWNER"
    user_by_email = var.owner_email
  }

  access {
    role          = "READER"
    special_group = "projectReaders"
  }

  access {
    role          = "WRITER"
    special_group = "projectWriters"
  }
}
