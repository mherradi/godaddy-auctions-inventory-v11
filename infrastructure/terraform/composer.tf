
# APIs required by Cloud Composer 3
resource "google_project_service" "composer_apis" {
  for_each = toset([
    "composer.googleapis.com",
    "compute.googleapis.com",
    "container.googleapis.com",
    "servicenetworking.googleapis.com"
  ])

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

# Dedicated identity for the Composer environment
resource "google_service_account" "composer" {
  project      = var.project_id
  account_id   = "godaddy-composer"
  display_name = "GoDaddy Auctions Composer Environment"
  description  = "Dedicated service account for Composer 3 development orchestration"

  depends_on = [
    google_project_service.composer_apis
  ]
}

# Composer worker permissions
resource "google_project_iam_member" "composer_worker" {
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.composer.email}"
}
