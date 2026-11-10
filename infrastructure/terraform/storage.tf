
resource "google_storage_bucket" "data_lake" {
  project                     = var.project_id
  name                        = "godaddy-auctions-v11-data-49757842710"
  location                    = "EUROPE-WEST9"
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = true
  }

  labels = {
    project     = "godaddy-auctions"
    environment = "dev"
    version     = "v11"
  }
}
