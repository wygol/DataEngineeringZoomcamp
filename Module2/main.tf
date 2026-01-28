terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.0.0"
      # 7.0.0 is the latest stable version
    }
  }
}

provider "google" {
  credentials = var.service_key
  project     = var.project
  region      = var.provider_region
}

resource "google_storage_bucket" "demo-bucket" {
  # General bucket settings
  name          = var.bucket_name
  location      = var.bucket_location
  force_destroy = true
  storage_class = var.storage_class

  # bucket security
  uniform_bucket_level_access = var.uniform_bucket_level_access
  public_access_prevention    = var.public_access_prevention


  # behaviour rules
  lifecycle_rule {
    condition {
      age = 8
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }

}