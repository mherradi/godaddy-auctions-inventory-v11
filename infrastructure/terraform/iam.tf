
# Cloud Run processing service account
resource "google_service_account" "processor" {
  project      = var.project_id
  account_id   = "godaddy-processor"
  display_name = "GoDaddy Auctions Processing Service Account"
  description  = "Identity used by the Cloud Run processing job"
}

# Allow the processor to read and write objects in the data lake
resource "google_storage_bucket_iam_member" "processor_storage" {
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.processor.email}"
}

# Allow the processor to load and modify BigQuery data
resource "google_project_iam_member" "processor_bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.processor.email}"
}

resource "google_bigquery_dataset_iam_member" "processor_bigquery_data_editor" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.source.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.processor.email}"
}


resource "google_project_iam_member" "processor_logging" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.processor.email}"
}

# Allow Composer to submit BigQuery load jobs
resource "google_project_iam_member" "composer_bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.composer.email}"
}

# Allow Composer to load data into the source dataset
resource "google_bigquery_dataset_iam_member" "composer_source_data_editor" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.source.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.composer.email}"
}

# Allow dbt to read source inventory and write transformed models
resource "google_bigquery_dataset_iam_member" "processor_dbt_data_editor" {
  project    = var.project_id
  dataset_id = "godaddy_auctions_dbt"
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.processor.email}"
}


resource "google_storage_bucket_iam_member" "composer_storage_viewer" {
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.composer.email}"
}