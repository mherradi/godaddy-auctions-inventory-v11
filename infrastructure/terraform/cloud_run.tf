resource "google_cloud_run_v2_job" "auction_processor" {
  project  = var.project_id
  name     = "godaddy-auctions-processor"
  location = var.region

  deletion_protection = true

  template {
    template {
      service_account = google_service_account.processor.email

      containers {
        image = "${var.region}-docker.pkg.dev/${var.project_id}/godaddy-auctions-v11/auction-processor@sha256:7b9b231ed58cde2d44a69073e46644282004257b2bfe589a9aa40e85d650897c"

        resources {
          limits = {
            cpu    = "2"
            memory = "4Gi"
          }
        }
      }

      max_retries = 1
      timeout     = "3600s"
    }
  }
}

resource "google_cloud_run_v2_job" "auction_dbt" {
  project  = var.project_id
  name     = "godaddy-auctions-dbt"
  location = var.region

  deletion_protection = true

  template {
    template {
      service_account = google_service_account.processor.email

      containers {
        image = "${var.region}-docker.pkg.dev/${var.project_id}/godaddy-auctions-v11/auction-dbt:v1.1.2"

        args = ["build"]

        resources {
          limits = {
            cpu    = "1"
            memory = "2Gi"
          }
        }
      }

      max_retries = 1
      timeout     = "3600s"
    }
  }
}