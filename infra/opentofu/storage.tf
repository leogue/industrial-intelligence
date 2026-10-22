variable "bucket_name" {
  description = "Nom du bucket GCS, unique à l’échelle mondiale."
  type        = string
}

variable "bucket_location" {
  description = "Localisation GCS : EU pour la multi-région européenne."
  type        = string
  default     = "EU"
}

resource "google_storage_bucket" "data" {
  name          = var.bucket_name
  project       = var.project_id
  location      = var.bucket_location
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  labels = {
    environment = "dev"
    managed_by  = "opentofu"
  }
}

output "bucket_name" {
  description = "Nom du bucket GCS."
  value       = google_storage_bucket.data.name
}

output "bucket_url" {
  description = "URI du bucket pour accéder aux données."
  value       = google_storage_bucket.data.url
}
