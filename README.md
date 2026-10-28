# Industrial Intelligence

Développement directement sur les services GCP du projet **`industrial-intelligence-dev`** : aucune base de données locale ni émulateur cloud obligatoire. Les conteneurs servent d’artefacts de déploiement Cloud Run, pas d’infrastructure locale.

## Outils

- Python 3.13 et `uv`
- Google Cloud CLI (`gcloud`)
- OpenTofu (`tofu`) pour l’infrastructure

## Accès au projet de développement

```sh
gcloud auth login
gcloud config set project industrial-intelligence-dev
gcloud auth application-default login
gcloud auth application-default set-quota-project industrial-intelligence-dev
uv sync
```

Les SDK Google utilisent les Application Default Credentials (ADC). Ne pas stocker de clés de compte de service dans le dépôt.

## Infrastructure

La configuration OpenTofu est dans `infra/opentofu/`. L’environnement de développement cible `industrial-intelligence-dev`.

```sh
cd infra/opentofu
tofu init
tofu fmt -check
tofu validate
# Dev déployé : conserver les deux fichiers, sinon les ressources RNE paraîtront désactivées.
tofu plan -var-file=envs/dev.tfvars -var-file=envs/rne-dev.tfvars -out=dev.tfplan
# tofu apply dev.tfplan  # uniquement après revue et approbation
tofu output bucket_url
```

### Bucket GCS de développement

- Projet : `industrial-intelligence-dev`.
- Bucket : `gs://industrial-intelligence-dev-data`.
- Localisation : `EU` (multi-région européenne, pas une région unique).
- Classe de stockage : `STANDARD`.
- Accès privé via IAM uniquement ; accès public interdit.
- `force_destroy = false` empêche OpenTofu de supprimer un bucket contenant des objets.

Le bucket a été créé via OpenTofu dans le projet de développement.

### Fichier temporaire pour les tests ETL

Un upload ponctuel du stock SIRENE des unités légales (publication du 1er octobre 2026, 713 585 991 octets) est disponible à la racine :

```text
gs://industrial-intelligence-dev-data/stock_unite_legale.parquet
```

Il provient de la ressource data.gouv.fr `350182c9-148a-46e0-8389-76c2ec1374a3`. La taille et les signatures Parquet ont été vérifiées avant l’upload. Ce fichier sert à explorer l’ETL ; ce n’est pas un pipeline d’ingestion et il n’est pas actualisé automatiquement. Ne pas l’écraser lors des tests.

### Configuration et état OpenTofu

Le nom et la localisation sont configurés dans `infra/opentofu/envs/dev.tfvars`. Les noms de buckets sont uniques au niveau mondial : si ce nom est déjà pris, le modifier avant la création. Pour une région européenne unique, utiliser par exemple `europe-west9` (Paris) à la place de `EU` **avant le premier déploiement** ; changer la localisation impose un remplacement du bucket.

Le compte utilisé doit disposer des droits de création et de gestion des buckets (par exemple `roles/storage.admin`) sur le projet, et l’API Cloud Storage (`storage.googleapis.com`) doit être activée. Si nécessaire, un administrateur peut l’activer avec :

```sh
gcloud services enable storage.googleapis.com --project=industrial-intelligence-dev
```

`tofu apply` crée une ressource facturable. Aucun déploiement n’est effectué automatiquement. L’état OpenTofu reste local pour cette première configuration : conserver `terraform.tfstate` hors de Git et le sauvegarder ; le bucket de données n’est pas un backend d’état.

## Ingestion INPI RNE

Le [guide d'ingestion RNE](docs/rne/inpi-ingestion.md) est la référence pour le fonctionnement, les commandes, le suivi et la mise en service.

Le stock national est archivé et converti : **5 470 fichiers, 27 348 472 lignes valides** (1 rejet, 822 répétitions de SIREN à examiner). Il correspond à environ mars 2026.

Trois commandes :

- **`bootstrap`** : archive le ZIP du FTP dans GCS (fait) ;
- **`materialize`** : convertit le ZIP en Parquet Bronze (fait) ;
- **`sync --start 2026-03-01T00:00:00Z`** : collecte les changements API depuis la borne supposée du stock, puis chaque jour la veille. Il n'y a qu'un seul checkpoint, et un job budgété qui reprend là où il s'est arrêté.

**Déployé en dev le 28/10/2026** : `rne-refresh` exécute `sync` toutes les 6 h et rattrape le trou depuis le 1er mars (≈ 2 semaines estimées si le quota INPI le permet). Suivi : événement `command_completed` dans Cloud Logging, champ `days_behind` ([détails](docs/rne/inpi-ingestion.md#suivi--logs-structurés-cloud-logging)).

```sh
uv sync --locked
uv run rne-ingest --help
uv run pytest
uv run ruff check .
uv run ruff format --check .
```

Les tests sont sans réseau. La CI GitHub (`.github/workflows/ci.yml`) lance ces vérifications sur chaque PR, ainsi que `tofu fmt -check` et `tofu validate` (sans identifiants GCP) ; `main` est protégée : passage par une PR et CI verte obligatoires. `.env.example` décrit les variables ; charger un `.env` privé explicitement avec `uv run --env-file .env ...`. Ne jamais intégrer les secrets à l'image ou aux variables OpenTofu.

Les fichiers `infra/opentofu/rne*.tf` définissent Artifact Registry, les secrets, les Cloud Run Jobs `rne-bootstrap`, `rne-refresh` (commande `sync`) et `rne-materialize`, ainsi que le Scheduler quotidien. Tout est désactivé par défaut et activé dans `envs/rne-dev.tfvars`. Publier l'image, appliquer un plan ou lancer un téléchargement national nécessite une validation explicite.

## Documentation

- **[RNE](docs/rne/)** : [guide principal d'ingestion](docs/rne/inpi-ingestion.md), exploration INPI, schéma Parquet, historique des données et coûts.
- **[SIRENE](docs/sirene/)** : [ingestion mensuelle data.gouv.fr](docs/sirene/sirene-ingestion.md) (commande `sirene-ingest`), [accès aux données](docs/sirene/sirene.md), dictionnaires des stocks et propositions de modélisation.
- **[Archives](docs/archives/)** : anciennes approches, audits, brouillon de contact INPI et journal des opérations. Ces documents ne sont pas des procédures actuelles.
