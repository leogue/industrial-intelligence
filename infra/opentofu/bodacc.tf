# Daily copy of the BODACC notices from the DILA API. Public source: no secret.
# Reuses the APIs enabled and the Artifact Registry repository created by rne.tf.
variable "enable_bodacc_ingestion" {
  description = "Créer le job bodacc-archive, son compte de service, ses droits GCS et son Scheduler."
  type        = bool
  default     = false
}

variable "bodacc_image" {
  description = "Image Dockerfile.bodacc épinglée par digest : REGION-docker.pkg.dev/PROJECT/rne/bodacc@sha256:..."
  type        = string
  default     = ""
  validation {
    condition     = var.bodacc_image == "" || can(regex("^[^[:space:]]+@sha256:[a-f0-9]{64}$", var.bodacc_image))
    error_message = "Utiliser une image immuable par digest, pas un tag latest."
  }
}

variable "bodacc_prefix" {
  description = "Namespace des objets : raw/PREFIX/ et control/PREFIX/. Pas de slash initial/final."
  type        = string
  default     = "dila/bodacc"
  validation {
    condition     = can(regex("^[a-zA-Z0-9_-]+(/[a-zA-Z0-9_-]+)*$", var.bodacc_prefix))
    error_message = "Le préfixe doit contenir des composants de chemin simples."
  }
}

variable "bodacc_schedule" {
  description = "Cron (UTC). La DILA publie vers 7 h ; le job collecte jusqu'à la veille et revérifie 30 jours."
  type        = string
  default     = "0 8 * * *"
}

resource "google_service_account" "bodacc_archive" {
  count        = var.enable_bodacc_ingestion ? 1 : 0
  project      = var.project_id
  account_id   = "bodacc-archive"
  display_name = "BODACC DILA API to raw (create/read only)"
}

# Originals may be created/read, never overwritten or deleted; only the checkpoint is mutable.
locals {
  bodacc_permissions = var.enable_bodacc_ingestion ? {
    raw_create = { role = "roles/storage.objectCreator", prefix = "raw/${var.bodacc_prefix}/" }
    raw_read   = { role = "roles/storage.objectViewer", prefix = "raw/${var.bodacc_prefix}/" }
    control    = { role = "roles/storage.objectUser", prefix = "control/${var.bodacc_prefix}/" }
  } : {}
}

resource "google_storage_bucket_iam_member" "bodacc_archive" {
  for_each = local.bodacc_permissions
  bucket   = google_storage_bucket.data.name
  role     = each.value.role
  member   = "serviceAccount:${google_service_account.bodacc_archive[0].email}"
  condition {
    title      = "bodacc_archive_${each.key}"
    expression = "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.data.name}/objects/${each.value.prefix}')"
  }
}

resource "google_cloud_run_v2_job" "bodacc_archive" {
  count               = var.enable_bodacc_ingestion ? 1 : 0
  project             = var.project_id
  name                = "bodacc-archive"
  location            = var.rne_region
  deletion_protection = true

  template {
    task_count  = 1
    parallelism = 1
    template {
      service_account = google_service_account.bodacc_archive[0].email
      timeout         = "21600s"
      max_retries     = 0
      containers {
        image = var.bodacc_image
        # The ~5.5 h soft budget ends before the 6 h timeout; the backfill resumes the next day.
        args = ["--max-seconds", "20000"]
        resources {
          limits = { cpu = "1", memory = "1Gi" }
        }
        env {
          name  = "BODACC_BUCKET"
          value = google_storage_bucket.data.name
        }
        env {
          name  = "BODACC_PREFIX"
          value = var.bodacc_prefix
        }
      }
    }
  }
  lifecycle {
    precondition {
      condition     = var.bodacc_image != ""
      error_message = "Publier l'image Dockerfile.bodacc et renseigner bodacc_image par digest."
    }
  }
  depends_on = [google_storage_bucket_iam_member.bodacc_archive]
}

resource "google_service_account" "bodacc_scheduler" {
  count        = var.enable_bodacc_ingestion ? 1 : 0
  project      = var.project_id
  account_id   = "bodacc-scheduler"
  display_name = "Scheduler: invoke only the BODACC archive job"
}

resource "google_cloud_run_v2_job_iam_member" "bodacc_scheduler" {
  count    = var.enable_bodacc_ingestion ? 1 : 0
  project  = var.project_id
  location = var.rne_region
  name     = google_cloud_run_v2_job.bodacc_archive[0].name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.bodacc_scheduler[0].email}"
}

resource "google_cloud_scheduler_job" "bodacc_archive" {
  count       = var.enable_bodacc_ingestion ? 1 : 0
  project     = var.project_id
  region      = var.rne_region
  name        = "bodacc-archive-daily"
  description = "Demande une exécution du job bodacc-archive ; HTTP 200 ne garantit pas le succès du job."
  schedule    = var.bodacc_schedule
  time_zone   = "Etc/UTC"

  attempt_deadline = "30s"
  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/${google_cloud_run_v2_job.bodacc_archive[0].id}:run"
    body        = base64encode("{}")
    headers     = { "Content-Type" = "application/json" }
    oauth_token {
      service_account_email = google_service_account.bodacc_scheduler[0].email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }
  depends_on = [google_cloud_run_v2_job_iam_member.bodacc_scheduler]
}
