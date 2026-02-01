# general provider settings
# --------------------------------------------------------------------------
variable "service_key" {
  description = "The path to the key to connect to the GCP account service"
  type        = string
  # DEFINE YOUR OWN KEY HERE or even better in your own "terraform.tfvars" file
  # and then gitignore that .tfvars file to keep your keys secure

}

variable "project" {
  description = "The unique project ID (must be unique across the entire GCP ecosystem!)"
  type        = string
  # DEFINE YOUR OWN PROJECT NAME HERE OR IN A terraform.tfvars
}

variable "provider_region" {
  description = "The region for the compute resources. Compute resources in the same multiregion as storage so that there are no egress fees."
  default     = "us-central1"
}
# --------------------------------------------------------------------------


# general bucket settings
# --------------------------------------------------------------------------
variable "bucket_name" {
  description = "Globally unique name for the bucket"
  # DEFINE YOUR OWN BUCKET NAME HERE OR IN A terraform.tfvars
}

variable "bucket_location" {
  description = "The bucket has to be in the US for EO data"
  default     = "US"
}

variable "storage_class" {
  description = "This was once MULTIREGION, but if location is just US and here is STANDARD, it knows that it is multiregion"
  default     = "STANDARD"
}
# --------------------------------------------------------------------------

# bucket security settings
# --------------------------------------------------------------------------
variable "uniform_bucket_level_access" {
  description = "This ensures that the new Uniform Bucket Level Access is used and not the old Access Control List"
  default     = true
}

variable "public_access_prevention" {
  description = "Prevents public access"
  default     = "enforced"

}
# --------------------------------------------------------------------------

# bigQuery setup
# --------------------------------------------------------------------------
variable "dataset_id" {
  description = "Dataset name (of an already existing bucket?)"
  default = "ny_taxi_data"

}




# --------------------------------------------------------------------------
