# IAM Service Accounts and Bindings

# Service account for Cloud Run services (runtime)
resource "google_service_account" "oxbow_runtime" {
  account_id   = "${local.service_name_prefix}-runtime"
  display_name = "Oxbow Cloud Run Runtime Service Account"
  description  = "Service account used by Oxbow Cloud Run services"
}

# Grant runtime service account read access to source bucket
resource "google_storage_bucket_iam_member" "runtime_source_reader" {
  bucket = var.source_bucket
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.oxbow_runtime.email}"
}

# Grant runtime service account write access to delta table bucket
resource "google_storage_bucket_iam_member" "runtime_delta_writer" {
  bucket = var.delta_table_bucket
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.oxbow_runtime.email}"
}

