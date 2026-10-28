# Optional manual extraction job. No source login/password, no schedule.
resource "google_service_account" "rne_materialize" {
  count        = var.enable_rne_materialization ? 1 : 0
  project      = var.project_id
  account_id   = "rne-materialize"
  display_name = "RNE stock ZIP to Bronze Parquet (no INPI credentials)"
  depends_on   = [google_project_service.rne]
}

locals {
  rne_materialize_permissions = var.enable_rne_materialization ? {
    raw_read      = { role = "roles/storage.objectViewer", prefix = "raw/${var.rne_prefix}/" }
    bronze_create = { role = "roles/storage.objectCreator", prefix = "bronze/${var.rne_prefix}/" }
    bronze_read   = { role = "roles/storage.objectViewer", prefix = "bronze/${var.rne_prefix}/" }
    control       = { role = "roles/storage.objectUser", prefix = "control/${var.rne_prefix}/" }
  } : {}
}

resource "google_storage_bucket_iam_member" "rne_materialize" {
  for_each = local.rne_materialize_permissions
  bucket   = google_storage_bucket.data.name
  role     = each.value.role
  member   = "serviceAccount:${google_service_account.rne_materialize[0].email}"
  condition {
    title      = "rne_materialize_${each.key}"
    expression = "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.data.name}/objects/${each.value.prefix}')"
  }
}

resource "google_cloud_run_v2_job" "rne_materialize" {
  count               = var.enable_rne_materialization ? 1 : 0
  project             = var.project_id
  name                = "rne-materialize"
  location            = var.rne_region
  deletion_protection = true
  template {
    task_count  = 1
    parallelism = 1
    template {
      service_account = google_service_account.rne_materialize[0].email
      timeout         = "21600s"
      max_retries     = 0
      containers {
        image = var.rne_materialize_image
        # Safe initial deployment: one new member, at most 128 MiB, 30 min soft budget.
        args = ["materialize", "--stock-revision", var.rne_stock_revision, "--max-members", "1"]
        resources {
          limits = { cpu = "1", memory = "2Gi" }
        }
        env {
          name  = "RNE_BUCKET"
          value = google_storage_bucket.data.name
        }
        env {
          name  = "RNE_PREFIX"
          value = var.rne_prefix
        }
      }
    }
  }
  lifecycle {
    precondition {
      condition     = var.enable_rne_ingestion && var.rne_materialize_image != "" && var.rne_stock_revision != ""
      error_message = "Activer les prérequis d'ingestion, publier une image materialize par digest et choisir une révision de stock."
    }
  }
  depends_on = [google_project_service.rne, google_storage_bucket_iam_member.rne_materialize]
}
