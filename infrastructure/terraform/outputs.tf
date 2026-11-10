output "project_id" {
  description = "Configured Google Cloud project"
  value       = var.project_id
}

output "region" {
  description = "Configured deployment region"
  value       = var.region
}

output "environment" {
  description = "Configured environment"
  value       = var.environment
}

output "artifact_registry_repository" {
  description = "Artifact Registry Docker repository"
  value       = google_artifact_registry_repository.docker_repo.name
}