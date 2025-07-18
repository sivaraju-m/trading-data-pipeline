terraform {
  backend "gcs" {
    bucket = "ai-trading-machine-tfstate" # Ensure this matches the bucket name
    prefix = "terraform/state"
  }
}
