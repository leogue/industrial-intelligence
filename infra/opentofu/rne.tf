# Entirely opt-in. These definitions do not imply that services are deployed.
locals {
  rne_services = toset(concat(
    var.enable_rne_prerequisites || var.enable_rne_ingestion ? [
      "artifactregistry.googleapis.com", "secretmanager.googleapis.com"
    ] : [],
    var.enable_rne_ingestion ? [
      "run.googleapis.com", "cloudscheduler.googleapis.com", "iam.googleapis.com"
    ] : []
  ))
  rne_jobs = var.enable_rne_ingestion ? {
    bootstrap = {
      timeout = "86400s"
      args    = ["bootstrap", "--allow-large-download"]
    }
    # Same command on every run: catches up from the stock boundary, then collects D-1.
    # The ~5.5 h soft budget ends before the 6 h timeout and the next 6-hourly run.
    refresh = {
      timeout = "21600s"
      args    = ["sync", "--start", var.rne_sync_start, "--max-requests", "10000", "--max-seconds", "20000"]
    }
  } : {}
  rne_secrets = var.enable_rne_prerequisites || var.enable_rne_ingestion ? {
    ftp_username = { job = "bootstrap", env = "INPI_FTP_USERNAME" }
    ftp_password = { job = "bootstrap", env = "INPI_FTP_PASSWORD" }
    api_username = { job = "refresh", env = "INPI_USERNAME" }
    api_password = { job = "refresh", env = "INPI_PASSWORD" }
  } : {}
}

resource "google_project_service" "rne" {
  for_each           = local.rne_services
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

resource "google_artifact_registry_repository" "rne" {
  count         = var.enable_rne_prerequisites || var.enable_rne_ingestion ? 1 : 0
  project       = var.project_id
  location      = var.rne_region
  repository_id = "rne"
  description   = "Image commune aux jobs d'ingestion RNE"
  format        = "DOCKER"
  depends_on    = [google_project_service.rne]
}

resource "google_service_account" "rne_job" {
  for_each     = local.rne_jobs
  project      = var.project_id
  account_id   = "rne-${each.key}"
  display_name = "RNE ${each.key} (raw ingestion only)"
  depends_on   = [google_project_service.rne]
}

# Do not put secret versions/payloads in Terraform: values stay out of tfvars/state.
resource "google_secret_manager_secret" "rne" {
  for_each  = local.rne_secrets
  project   = var.project_id
  secret_id = "inpi-rne-${replace(each.key, "_", "-")}"
  replication {
    auto {}
  }
  lifecycle {
    prevent_destroy = true
  }
  depends_on = [google_project_service.rne]
}

resource "google_secret_manager_secret_iam_member" "rne" {
  for_each  = var.enable_rne_ingestion ? local.rne_secrets : {}
  project   = var.project_id
  secret_id = google_secret_manager_secret.rne[each.key].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.rne_job[each.value.job].email}"
}

# Originals may be created/read, never overwritten or deleted by ingestion workers.
resource "google_storage_bucket_iam_member" "rne_raw_create" {
  for_each = local.rne_jobs
  bucket   = google_storage_bucket.data.name
  role     = "roles/storage.objectCreator"
  member   = "serviceAccount:${google_service_account.rne_job[each.key].email}"
  condition {
    title      = "rne_raw_create_only"
    expression = "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.data.name}/objects/raw/${var.rne_prefix}/')"
  }
}

resource "google_storage_bucket_iam_member" "rne_raw_read" {
  for_each = local.rne_jobs
  bucket   = google_storage_bucket.data.name
  role     = "roles/storage.objectViewer"
  member   = "serviceAccount:${google_service_account.rne_job[each.key].email}"
  condition {
    title      = "rne_raw_read_only"
    expression = "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.data.name}/objects/raw/${var.rne_prefix}/')"
  }
}

# Mutable state + conditional locks are confined to a separate prefix.
resource "google_storage_bucket_iam_member" "rne_control" {
  for_each = local.rne_jobs
  bucket   = google_storage_bucket.data.name
  role     = "roles/storage.objectUser"
  member   = "serviceAccount:${google_service_account.rne_job[each.key].email}"
  condition {
    title      = "rne_control_only"
    expression = "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.data.name}/objects/control/${var.rne_prefix}/')"
  }
}

resource "google_storage_bucket_iam_member" "rne_refresh_bronze" {
  for_each = var.enable_rne_ingestion ? {
    create = "roles/storage.objectCreator"
    read   = "roles/storage.objectViewer"
  } : {}
  bucket = google_storage_bucket.data.name
  role   = each.value
  member = "serviceAccount:${google_service_account.rne_job["refresh"].email}"
  condition {
    title      = "rne_refresh_bronze_${each.key}"
    expression = "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.data.name}/objects/bronze/${var.rne_prefix}/')"
  }
}

resource "google_cloud_run_v2_job" "rne" {
  for_each            = local.rne_jobs
  project             = var.project_id
  name                = "rne-${each.key}"
  location            = var.rne_region
  deletion_protection = true

  template {
    task_count  = 1
    parallelism = 1
    template {
      service_account = google_service_account.rne_job[each.key].email
      timeout         = each.value.timeout
      max_retries     = 0
      containers {
        image = var.rne_image
        args  = each.value.args
        resources {
          limits = { cpu = "1", memory = each.key == "refresh" ? "2Gi" : "1Gi" }
        }
        env {
          name  = "RNE_BUCKET"
          value = google_storage_bucket.data.name
        }
        env {
          name  = "RNE_PREFIX"
          value = var.rne_prefix
        }
        dynamic "env" {
          for_each = each.key == "bootstrap" ? {
            RNE_STOCK_FILE = var.rne_stock_file
          } : {}
          content {
            name  = env.key
            value = env.value
          }
        }
        dynamic "env" {
          for_each = { for key, secret in local.rne_secrets : key => secret if secret.job == each.key }
          content {
            name = env.value.env
            value_source {
              secret_key_ref {
                secret  = google_secret_manager_secret.rne[env.key].secret_id
                version = "latest"
              }
            }
          }
        }
      }
    }
  }
  lifecycle {
    precondition {
      condition     = var.rne_image != ""
      error_message = "Publier l'image et renseigner rne_image par digest avant d'activer les jobs."
    }
    precondition {
      condition     = var.rne_sync_start != ""
      error_message = "Renseigner rne_sync_start : borne supposée du stock, point de départ du différentiel."
    }
  }
  depends_on = [
    google_project_service.rne,
    google_secret_manager_secret_iam_member.rne,
    google_storage_bucket_iam_member.rne_raw_create,
    google_storage_bucket_iam_member.rne_raw_read,
    google_storage_bucket_iam_member.rne_control,
    google_storage_bucket_iam_member.rne_refresh_bronze
  ]
}

resource "google_service_account" "rne_scheduler" {
  count        = var.enable_rne_ingestion ? 1 : 0
  project      = var.project_id
  account_id   = "rne-scheduler"
  display_name = "Scheduler: invoke only the RNE refresh job"
  depends_on   = [google_project_service.rne]
}

resource "google_cloud_run_v2_job_iam_member" "rne_scheduler" {
  count    = var.enable_rne_ingestion ? 1 : 0
  project  = var.project_id
  location = var.rne_region
  name     = google_cloud_run_v2_job.rne["refresh"].name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.rne_scheduler[0].email}"
}

resource "google_cloud_scheduler_job" "rne_refresh" {
  count       = var.enable_rne_ingestion ? 1 : 0
  project     = var.project_id
  region      = var.rne_region
  name        = "rne-refresh-daily"
  description = "Demande une exécution du job différentiel ; HTTP 200 ne garantit pas le succès du job."
  schedule    = var.rne_schedule
  time_zone   = "Etc/UTC"
  paused      = !var.rne_schedule_enabled

  attempt_deadline = "30s"
  # Defaults retryCount=0 + maxRetryDuration=0 disable retries between cron runs.
  # The API omits an all-zero retry_config; declaring it causes a permanent diff.
  # https://cloud.google.com/scheduler/docs/configuring/retry-jobs
  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/${google_cloud_run_v2_job.rne["refresh"].id}:run"
    body        = base64encode("{}")
    headers     = { "Content-Type" = "application/json" }
    oauth_token {
      service_account_email = google_service_account.rne_scheduler[0].email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }
  depends_on = [google_cloud_run_v2_job_iam_member.rne_scheduler]
}

output "rne_repository_url" {
  description = "Préfixe de publication des images, si le registre a été activé."
  value       = try("${var.rne_region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.rne[0].repository_id}", null)
}

output "rne_job_names" {
  value = { for key, job in google_cloud_run_v2_job.rne : key => job.name }
}
