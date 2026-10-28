# Monthly copy of the SIRENE Parquet stocks from data.gouv.fr. Public source: no secret.
# Reuses the APIs enabled and the Artifact Registry repository created by rne.tf.
variable "enable_sirene_ingestion" {
  description = "Créer le job sirene-archive, son compte de service, ses droits GCS et son Scheduler."
  type        = bool
  default     = false
}

variable "sirene_image" {
  description = "Image Dockerfile.sirene épinglée par digest : REGION-docker.pkg.dev/PROJECT/rne/sirene@sha256:..."
  type        = string
  default     = ""
  validation {
    condition     = var.sirene_image == "" || can(regex("^[^[:space:]]+@sha256:[a-f0-9]{64}$", var.sirene_image))
    error_message = "Utiliser une image immuable par digest, pas un tag latest."
  }
}

variable "sirene_prefix" {
  description = "Namespace des objets : bronze/PREFIX/. Pas de slash initial/final."
  type        = string
  default     = "insee/sirene"
  validation {
    condition     = can(regex("^[a-zA-Z0-9_-]+(/[a-zA-Z0-9_-]+)*$", var.sirene_prefix))
    error_message = "Le préfixe doit contenir des composants de chemin simples."
  }
}

variable "sirene_schedule" {
  description = "Cron (UTC). Une exécution déjà à jour ne fait qu'un appel au catalogue : un run quotidien rattrape seul un échec ou une publication tardive."
  type        = string
  default     = "0 6 * * *"
}

resource "google_service_account" "sirene_archive" {
  count        = var.enable_sirene_ingestion ? 1 : 0
  project      = var.project_id
  account_id   = "sirene-archive"
  display_name = "SIRENE data.gouv.fr to Bronze (create/read only)"
}

# Originals may be created/read, never overwritten or deleted by the job.
resource "google_storage_bucket_iam_member" "sirene_archive" {
  for_each = var.enable_sirene_ingestion ? {
    create = "roles/storage.objectCreator"
    read   = "roles/storage.objectViewer"
  } : {}
  bucket = google_storage_bucket.data.name
  role   = each.value
  member = "serviceAccount:${google_service_account.sirene_archive[0].email}"
  condition {
    title      = "sirene_archive_bronze_${each.key}"
    expression = "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.data.name}/objects/bronze/${var.sirene_prefix}/')"
  }
}

resource "google_cloud_run_v2_job" "sirene_archive" {
  count               = var.enable_sirene_ingestion ? 1 : 0
  project             = var.project_id
  name                = "sirene-archive"
  location            = var.rne_region
  deletion_protection = true

  template {
    task_count  = 1
    parallelism = 1
    template {
      service_account = google_service_account.sirene_archive[0].email
      timeout         = "21600s"
      max_retries     = 0
      containers {
        image = var.sirene_image
        args  = ["--allow-large-download"]
        resources {
          limits = { cpu = "1", memory = "1Gi" }
        }
        env {
          name  = "SIRENE_BUCKET"
          value = google_storage_bucket.data.name
        }
        env {
          name  = "SIRENE_PREFIX"
          value = var.sirene_prefix
        }
      }
    }
  }
  lifecycle {
    precondition {
      condition     = var.sirene_image != ""
      error_message = "Publier l'image Dockerfile.sirene et renseigner sirene_image par digest."
    }
  }
  depends_on = [google_storage_bucket_iam_member.sirene_archive]
}

resource "google_service_account" "sirene_scheduler" {
  count        = var.enable_sirene_ingestion ? 1 : 0
  project      = var.project_id
  account_id   = "sirene-scheduler"
  display_name = "Scheduler: invoke only the SIRENE archive job"
}

resource "google_cloud_run_v2_job_iam_member" "sirene_scheduler" {
  count    = var.enable_sirene_ingestion ? 1 : 0
  project  = var.project_id
  location = var.rne_region
  name     = google_cloud_run_v2_job.sirene_archive[0].name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.sirene_scheduler[0].email}"
}

resource "google_cloud_scheduler_job" "sirene_archive" {
  count       = var.enable_sirene_ingestion ? 1 : 0
  project     = var.project_id
  region      = var.rne_region
  name        = "sirene-archive-daily"
  description = "Demande une exécution du job sirene-archive ; HTTP 200 ne garantit pas le succès du job."
  schedule    = var.sirene_schedule
  time_zone   = "Etc/UTC"

  attempt_deadline = "30s"
  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/${google_cloud_run_v2_job.sirene_archive[0].id}:run"
    body        = base64encode("{}")
    headers     = { "Content-Type" = "application/json" }
    oauth_token {
      service_account_email = google_service_account.sirene_scheduler[0].email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }
  depends_on = [google_cloud_run_v2_job_iam_member.sirene_scheduler]
}
