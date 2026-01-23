# Terraform configuration for deploying Oxbow file-loader on Google Cloud Platform
#
# This deployment creates:
# - GCS bucket for source files and Delta tables
# - Cloud Run service (file-loader) that polls for JSONL files
# - IAM bindings

terraform {
  required_version = ">= 1.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Variables
variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "region" {
  description = "The GCP region for resources"
  type        = string
  default     = "us-central1"
}

variable "source_bucket" {
  description = "GCS bucket to poll for source files"
  type        = string
}

variable "delta_table_bucket" {
  description = "GCS bucket for Delta table output (can be same as source_bucket)"
  type        = string
}

variable "environment" {
  description = "Environment name (e.g., dev, staging, prod)"
  type        = string
  default     = "dev"
}

# File Loader configuration
variable "file_loader_source_prefix" {
  description = "GCS prefix to poll for JSONL files (e.g., 'logs/' or 'incoming/')"
  type        = string
  default     = "incoming/"
}

variable "file_loader_delta_table_path" {
  description = "Path within the bucket for the destination Delta table"
  type        = string
  default     = "delta-tables/logs"
}

variable "file_loader_state_file_path" {
  description = "Path within the bucket for the file-loader state file"
  type        = string
  default     = ".file-loader-state/processed.json"
}

variable "file_loader_image" {
  description = "Container image"
  type        = string
  default     = "asia-northeast1-docker.pkg.dev/data-dev-596660/main-asia/oxbow/blizzard:latest"
}

variable "file_loader_poll_interval_secs" {
  description = "Polling interval in seconds"
  type        = number
  default     = 10
}

variable "file_loader_listing_lookback_hours" {
  description = "Only list files from last N hours (reduces listing time for large buckets)"
  type        = number
  default     = null
}

# Local values
locals {
  service_name_prefix = "oxbow-${var.environment}"

  common_labels = {
    managed_by  = "terraform"
    environment = var.environment
    project     = "oxbow"
  }
}

# Enable required APIs
resource "google_project_service" "required_apis" {
  for_each = toset([
    "run.googleapis.com",
    "storage.googleapis.com",
  ])

  service            = each.value
  disable_on_destroy = false
}

# Outputs
output "file_loader_service_url" {
  description = "URL of the file-loader Cloud Run service"
  value       = google_cloud_run_v2_service.file_loader.uri
}

output "service_account_email" {
  description = "Service account email for the file-loader"
  value       = google_service_account.oxbow_runtime.email
}

