project_id      = "industrial-intelligence-dev"
bucket_name     = "industrial-intelligence-dev-data"
bucket_location = "EU"

# SIRENE : copie data.gouv.fr -> bronze/insee/sirene/, Scheduler quotidien (no-op si déjà à jour).
enable_sirene_ingestion = true
sirene_image            = "europe-west9-docker.pkg.dev/industrial-intelligence-dev/rne/sirene@sha256:c373df8bde2ef15dae35abb497921f1b6f538586fd7567768452851324216803"

# BODACC : annonces commerciales DILA -> raw/dila/bodacc/, depuis 2008, Scheduler quotidien.
# Image du commit 06286a3.
enable_bodacc_ingestion = true
bodacc_image            = "europe-west9-docker.pkg.dev/industrial-intelligence-dev/rne/bodacc@sha256:f398b9b0f0fceac18c4ed644715f00870fc0737c240adb0e7b6ccc081720e011"

# Silver -> Iceberg : image du commit d97be7b, Scheduler quotidien à 12:00 UTC.
enable_lakehouse_load = true
lakehouse_image       = "europe-west9-docker.pkg.dev/industrial-intelligence-dev/rne/lakehouse@sha256:5df15229681b2d51a3f027d0d03005400993f1ce87cc85d363e82fd5aac855f5"

# Spark ETL : réseau, compte etl-spark, catalogue Iceberg BigLake et namespaces silver_spark/gold.
enable_lakehouse = true
