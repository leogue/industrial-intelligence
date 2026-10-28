project_id      = "industrial-intelligence-dev"
bucket_name     = "industrial-intelligence-dev-data"
bucket_location = "EU"

# SIRENE : copie data.gouv.fr -> bronze/insee/sirene/, Scheduler quotidien (no-op si déjà à jour).
enable_sirene_ingestion = true
sirene_image            = "europe-west9-docker.pkg.dev/industrial-intelligence-dev/rne/sirene@sha256:c373df8bde2ef15dae35abb497921f1b6f538586fd7567768452851324216803"
