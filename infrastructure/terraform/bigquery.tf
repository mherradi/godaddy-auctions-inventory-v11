
resource "google_bigquery_dataset" "source" {
  project                    = var.project_id
  dataset_id                 = "godaddy_auctions"
  location                   = var.region
  description                = "GoDaddy Auctions V1.1 source inventory dataset"
  delete_contents_on_destroy = false

  labels = {
    project     = "godaddy-auctions"
    environment = "dev"
    version     = "v11"
  }
}
