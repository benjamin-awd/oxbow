# Cloud Run Services

# File Loader Cloud Run service (polling-based)
# Polls a GCS bucket for JSONL files and loads them into a Delta table
resource "google_cloud_run_v2_service" "file_loader" {
  name     = "${local.service_name_prefix}-file-loader"
  location = var.region
  deletion_protection = false

  labels = local.common_labels

  ingress = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  template {
    service_account = google_service_account.oxbow_runtime.email

    scaling {
      # Keep at least 1 instance running for continuous polling
      min_instance_count = 1
      max_instance_count = 1
    }

    timeout = "3600s" # 1 hour max for long-running polling

    containers {
      image = var.file_loader_image

      resources {
        limits = {
          cpu    = "1000m"
          memory = "2Gi"
        }
        # CPU always allocated since this is a polling service
        cpu_idle = false
      }

      env {
        name  = "RUST_LOG"
        value = "file_loader=info,oxbow=info,deltalake=warn"
      }

      env {
        name  = "SOURCE_BUCKET"
        value = var.source_bucket
      }

      env {
        name  = "SOURCE_PREFIX"
        value = var.file_loader_source_prefix
      }

      env {
        name  = "DELTA_TABLE_URI"
        value = "gs://${var.delta_table_bucket}/${var.file_loader_delta_table_path}"
      }

      env {
        name  = "STATE_FILE_URI"
        value = "gs://${var.delta_table_bucket}/${var.file_loader_state_file_path}"
      }

      env {
        name  = "POLL_INTERVAL_SECS"
        value = tostring(var.file_loader_poll_interval_secs)
      }

      dynamic "env" {
        for_each = var.file_loader_listing_lookback_hours != null ? [1] : []
        content {
          name  = "LISTING_LOOKBACK_HOURS"
          value = tostring(var.file_loader_listing_lookback_hours)
        }
      }

      ports {
        container_port = 8080
      }

      startup_probe {
        http_get {
          path = "/health"
        }
        initial_delay_seconds = 0
        timeout_seconds       = 1
        period_seconds        = 3
        failure_threshold     = 10
      }

      liveness_probe {
        http_get {
          path = "/health"
        }
        initial_delay_seconds = 0
        timeout_seconds       = 1
        period_seconds        = 30
        failure_threshold     = 3
      }
    }
  }

  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }

  depends_on = [google_project_service.required_apis]
}
