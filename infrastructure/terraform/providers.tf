
terraform {
  required_version = ">= 1.9.0"

  backend "gcs" {
    bucket = "godaddy-auctions-v11-tfstate-49757842710"
    prefix = "terraform/state"
  }

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}
