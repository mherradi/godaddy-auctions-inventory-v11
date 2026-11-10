variable "project_id" {
  description = "Google Cloud project ID"
  type        = string
  default     = "godaddy-auctions-inventory-v11"
}

variable "region" {
  description = "Primary Google Cloud region"
  type        = string
  default     = "europe-west9"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}

variable "container_image" {
  description = "Container image used by the Cloud Run processing job"
  type        = string
  default     = "latest"
}