
resource "google_artifact_registry_repository" "docker_repo" {
  project       = var.project_id
  location      = var.region
  repository_id = "godaddy-auctions-v11"
  description   = "Docker images for GoDaddy Auctions V1.1"
  format        = "DOCKER"

  docker_config {
    immutable_tags = true
  }
}
