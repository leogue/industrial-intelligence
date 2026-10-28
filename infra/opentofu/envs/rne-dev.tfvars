# Surcharge RNE de développement : utiliser APRÈS envs/dev.tfvars.
# Ce fichier ne contient aucune valeur secrète et ne déclenche aucun job.
# Image du commit deac8a8 (sync simplifié + logs structurés), publiée par digest.
enable_rne_prerequisites = true
enable_rne_ingestion     = true
rne_schedule_enabled     = true
rne_region               = "europe-west9"
rne_prefix               = "sandbox/leo/inpi/rne"
rne_image                = "europe-west9-docker.pkg.dev/industrial-intelligence-dev/rne/ingestion@sha256:b65d416cfb6861a8954023c67944442e483f6e5ddc6e7c111af208d037eeb068"

rne_stock_file = "stock_RNE_formalites_NIVEAU1_20260304_1400.zip"

# Hypothèse d'équipe, non certifiée par l'INPI : le stock (membres ZIP du 5 mars 2026)
# couvre les changements jusqu'au 1er mars. Le différentiel repart de cette borne.
rne_sync_start = "2026-03-01T00:00:00Z"

# Toutes les 6 h : ~4 × 10 000 appels/jour pendant le rattrapage (~12 jours estimés).
# Une fois à jour, les exécutions sans retard se terminent sans appel API.
rne_schedule = "0 */6 * * *"

# Worker ZIP GCS -> Parquet, sans secret INPI, exécutions manuelles.
# Le défaut du job reste borné à un membre ; le lancement national utilise des overrides.
enable_rne_materialization = true
rne_materialize_image      = "europe-west9-docker.pkg.dev/industrial-intelligence-dev/rne/ingestion@sha256:b65d416cfb6861a8954023c67944442e483f6e5ddc6e7c111af208d037eeb068"
rne_stock_revision         = "b47860c7c6a645bc1d54e878d2c4d2b5ec67b2d4530da9b1bd5f2cd2c8c9fbde"
