# Publishes the latest complete Silver run as Iceberg tables in the BigLake catalog.
# The catalog (industrial_intelligence, namespace silver, data under gs://BUCKET/iceberg/) is
# declared in spark.tf; this file only references it by name.
# Reuses the APIs enabled and the Artifact Registry repository created by rne.tf.
variable "enable_lakehouse_load" {
  description = "Créer le job lakehouse-load, son compte de service, ses droits et son Scheduler."
  type        = bool
  default     = false
}

variable "lakehouse_image" {
  description = "Image Dockerfile.lakehouse épinglée par digest : REGION-docker.pkg.dev/PROJECT/rne/lakehouse@sha256:..."
  type        = string
  default     = ""
  validation {
    condition     = var.lakehouse_image == "" || can(regex("^[^[:space:]]+@sha256:[a-f0-9]{64}$", var.lakehouse_image))
    error_message = "Utiliser une image immuable par digest, pas un tag latest."
  }
}

variable "lakehouse_catalog" {
  description = "Catalogue BigLake Iceberg (existant) : bl://projects/PROJECT/catalogs/CATALOG."
  type        = string
  default     = "industrial_intelligence"
}

variable "lakehouse_namespace" {
  description = "Namespace Iceberg ; BigQuery lit PROJECT.CATALOG.NAMESPACE.TABLE."
  type        = string
  default     = "silver"
}

variable "lakehouse_silver_prefix" {
  description = "Préfixe des runs Silver lus (PREFIX/RUN/_manifest.json). Pas de slash initial/final."
  type        = string
  default     = "silver/samples"
  validation {
    condition     = can(regex("^[a-zA-Z0-9_-]+(/[a-zA-Z0-9_-]+)*$", var.lakehouse_silver_prefix))
    error_message = "Le préfixe doit contenir des composants de chemin simples."
  }
}

variable "lakehouse_schedule" {
  description = "Cron (UTC). Un run déjà publié ne relit que les manifestes : quelques secondes."
  type        = string
  default     = "0 12 * * *"
}

locals {
  lakehouse_objects = "projects/_/buckets/${google_storage_bucket.data.name}/objects"
}

resource "google_service_account" "lakehouse_load" {
  count        = var.enable_lakehouse_load ? 1 : 0
  project      = var.project_id
  account_id   = "lakehouse-load"
  display_name = "Silver to Iceberg (read silver/, create iceberg/)"
}

# Listing is authorized on the bucket resource itself, hence the bucket name in the read
# condition. Iceberg only adds files: no overwrite or delete right on iceberg/.
resource "google_storage_bucket_iam_member" "lakehouse_load" {
  for_each = var.enable_lakehouse_load ? {
    read = {
      role       = "roles/storage.objectViewer"
      expression = "resource.name == 'projects/_/buckets/${google_storage_bucket.data.name}' || resource.name.startsWith('${local.lakehouse_objects}/${var.lakehouse_silver_prefix}/') || resource.name.startsWith('${local.lakehouse_objects}/iceberg/')"
    }
    create = {
      role       = "roles/storage.objectCreator"
      expression = "resource.name.startsWith('${local.lakehouse_objects}/iceberg/')"
    }
  } : {}
  bucket = google_storage_bucket.data.name
  role   = each.value.role
  member = "serviceAccount:${google_service_account.lakehouse_load[0].email}"
  condition {
    title      = "lakehouse_load_${each.key}"
    expression = each.value.expression
  }
}

# Catalog commits (BigLake) and the x-goog-user-project quota header (Service Usage).
resource "google_project_iam_member" "lakehouse_load" {
  for_each = var.enable_lakehouse_load ? toset([
    "roles/biglake.editor", "roles/serviceusage.serviceUsageConsumer"
  ]) : toset([])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.lakehouse_load[0].email}"
}

resource "google_cloud_run_v2_job" "lakehouse_load" {
  count               = var.enable_lakehouse_load ? 1 : 0
  project             = var.project_id
  name                = "lakehouse-load"
  location            = var.rne_region
  deletion_protection = true

  template {
    task_count  = 1
    parallelism = 1
    template {
      service_account = google_service_account.lakehouse_load[0].email
      timeout         = "3600s"
      max_retries     = 0
      containers {
        image = var.lakehouse_image
        resources {
          limits = { cpu = "1", memory = "2Gi" }
        }
        env {
          name  = "LAKEHOUSE_BUCKET"
          value = google_storage_bucket.data.name
        }
        env {
          name  = "LAKEHOUSE_SILVER_PREFIX"
          value = var.lakehouse_silver_prefix
        }
        env {
          name  = "LAKEHOUSE_PROJECT"
          value = var.project_id
        }
        env {
          name  = "LAKEHOUSE_CATALOG"
          value = var.lakehouse_catalog
        }
        env {
          name  = "LAKEHOUSE_NAMESPACE"
          value = var.lakehouse_namespace
        }
      }
    }
  }
  lifecycle {
    precondition {
      condition     = var.lakehouse_image != ""
      error_message = "Publier l'image Dockerfile.lakehouse et renseigner lakehouse_image par digest."
    }
  }
  depends_on = [google_storage_bucket_iam_member.lakehouse_load, google_project_iam_member.lakehouse_load]
}

resource "google_service_account" "lakehouse_scheduler" {
  count        = var.enable_lakehouse_load ? 1 : 0
  project      = var.project_id
  account_id   = "lakehouse-scheduler"
  display_name = "Scheduler: invoke only the lakehouse-load job"
}

resource "google_cloud_run_v2_job_iam_member" "lakehouse_scheduler" {
  count    = var.enable_lakehouse_load ? 1 : 0
  project  = var.project_id
  location = var.rne_region
  name     = google_cloud_run_v2_job.lakehouse_load[0].name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.lakehouse_scheduler[0].email}"
}

resource "google_cloud_scheduler_job" "lakehouse_load" {
  count       = var.enable_lakehouse_load ? 1 : 0
  project     = var.project_id
  region      = var.rne_region
  name        = "lakehouse-load-daily"
  description = "Demande une exécution du job lakehouse-load ; HTTP 200 ne garantit pas le succès du job."
  schedule    = var.lakehouse_schedule
  time_zone   = "Etc/UTC"

  attempt_deadline = "30s"
  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/${google_cloud_run_v2_job.lakehouse_load[0].id}:run"
    body        = base64encode("{}")
    headers     = { "Content-Type" = "application/json" }
    oauth_token {
      service_account_email = google_service_account.lakehouse_scheduler[0].email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }
  depends_on = [google_cloud_run_v2_job_iam_member.lakehouse_scheduler]
}
