# Spark ETL (Serverless for Apache Spark) writing Iceberg tables registered in the BigLake
# (Lakehouse) Iceberg REST catalog. Data files stay in the project bucket under iceberg/.
# Created on 2026-10-29 from branch feat/lakehouse-spark-iceberg; declared again here with the
# same addresses so that the plan stops proposing to destroy them.
# Batches are submitted by hand (docs/etl/spark-graphe.md): no Scheduler yet.
variable "enable_lakehouse" {
  description = "Créer le réseau Spark, le compte de service etl-spark et le catalogue Iceberg BigLake."
  type        = bool
  default     = false
}

variable "lakehouse_region" {
  description = "Région des batchs Spark et de leur sous-réseau ; européenne, comme le bucket EU."
  type        = string
  default     = "europe-west9"
}

locals {
  lakehouse_services = var.enable_lakehouse ? toset([
    "compute.googleapis.com", "dataproc.googleapis.com", "biglake.googleapis.com"
  ]) : toset([])
  lakehouse_subnet_cidr = "10.20.0.0/24"
}

resource "google_project_service" "lakehouse" {
  for_each           = local.lakehouse_services
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

# Spark workers have internal IPs only: Private Google Access reaches GCS and BigLake,
# and the runtime 2.x needs open internal traffic between workers. No NAT: no internet.
resource "google_compute_network" "lakehouse" {
  count                   = var.enable_lakehouse ? 1 : 0
  project                 = var.project_id
  name                    = "lakehouse"
  auto_create_subnetworks = false
  depends_on              = [google_project_service.lakehouse]
}

resource "google_compute_subnetwork" "lakehouse" {
  count                    = var.enable_lakehouse ? 1 : 0
  project                  = var.project_id
  name                     = "lakehouse-${var.lakehouse_region}"
  region                   = var.lakehouse_region
  network                  = google_compute_network.lakehouse[0].id
  ip_cidr_range            = local.lakehouse_subnet_cidr
  private_ip_google_access = true
}

resource "google_compute_firewall" "lakehouse_internal" {
  count         = var.enable_lakehouse ? 1 : 0
  project       = var.project_id
  name          = "lakehouse-spark-internal"
  network       = google_compute_network.lakehouse[0].id
  direction     = "INGRESS"
  source_ranges = [local.lakehouse_subnet_cidr]
  allow {
    protocol = "all"
  }
}

resource "google_service_account" "etl_spark" {
  count        = var.enable_lakehouse ? 1 : 0
  project      = var.project_id
  account_id   = "etl-spark"
  display_name = "Spark ETL: reads the bucket, writes Iceberg tables under iceberg/"
}

resource "google_project_iam_member" "etl_spark" {
  for_each = var.enable_lakehouse ? toset([
    "roles/dataproc.worker", "roles/biglake.editor", "roles/serviceusage.serviceUsageConsumer"
  ]) : toset([])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.etl_spark[0].email}"
}

# Spark lists directories, which a resource.name condition cannot grant: read is bucket-wide.
resource "google_storage_bucket_iam_member" "etl_spark_read" {
  count  = var.enable_lakehouse ? 1 : 0
  bucket = google_storage_bucket.data.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.etl_spark[0].email}"
}

# End-user credential mode: the catalog uses the caller's own access to the data files.
resource "google_storage_bucket_iam_member" "etl_spark_iceberg" {
  count  = var.enable_lakehouse ? 1 : 0
  bucket = google_storage_bucket.data.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.etl_spark[0].email}"
  condition {
    title      = "etl_spark_iceberg_only"
    expression = "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.data.name}/objects/iceberg/')"
  }
}

resource "google_biglake_iceberg_catalog" "lakehouse" {
  count            = var.enable_lakehouse ? 1 : 0
  project          = var.project_id
  name             = "industrial_intelligence"
  catalog_type     = "CATALOG_TYPE_BIGLAKE"
  credential_mode  = "CREDENTIAL_MODE_END_USER"
  default_location = "gs://${google_storage_bucket.data.name}/iceberg"
  # EU lets BigQuery query the tables from its EU multi-region, like the bucket.
  primary_location = "EU"
  deletion_policy  = "PREVENT"
  depends_on       = [google_project_service.lakehouse]
}

resource "google_biglake_iceberg_namespace" "silver" {
  count           = var.enable_lakehouse ? 1 : 0
  project         = var.project_id
  catalog         = google_biglake_iceberg_catalog.lakehouse[0].name
  namespace_id    = "silver"
  properties      = { location = "gs://${google_storage_bucket.data.name}/iceberg/silver" }
  deletion_policy = "PREVENT"
}

# Written by the Spark ETL (etl/graph.py); `silver` stays the namespace of lakehouse-load.
resource "google_biglake_iceberg_namespace" "etl" {
  for_each        = var.enable_lakehouse ? toset(["silver_spark", "gold"]) : toset([])
  project         = var.project_id
  catalog         = google_biglake_iceberg_catalog.lakehouse[0].name
  namespace_id    = each.value
  properties      = { location = "gs://${google_storage_bucket.data.name}/iceberg/${each.value}" }
  deletion_policy = "PREVENT"
}

output "lakehouse_catalog" {
  value = try(google_biglake_iceberg_catalog.lakehouse[0].name, null)
}

output "lakehouse_subnet" {
  value = try(google_compute_subnetwork.lakehouse[0].name, null)
}
