# Industrial Intelligence

Développement directement sur les services GCP du projet **`industrial-intelligence-dev`** : pas de Docker, de base de données locale ni d’émulateurs cloud.

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
tofu plan -var-file=envs/dev.tfvars -out=dev.tfplan
tofu apply dev.tfplan
tofu output bucket_url
```

### Bucket GCS de développement

- Projet : `industrial-intelligence-dev`.
- Bucket : `gs://industrial-intelligence-dev-data`.
- Localisation : `EU` (multi-région européenne, pas une région unique).
- Classe de stockage : `STANDARD`.
- Accès privé via IAM uniquement ; accès public interdit.
- `force_destroy = false` empêche OpenTofu de supprimer un bucket contenant des objets.

Le nom et la localisation sont configurés dans `infra/opentofu/envs/dev.tfvars`. Les noms de buckets sont uniques au niveau mondial : si ce nom est déjà pris, le modifier avant la création. Pour une région européenne unique, utiliser par exemple `europe-west9` (Paris) à la place de `EU` **avant le premier déploiement** ; changer la localisation impose un remplacement du bucket.

Le compte utilisé doit disposer des droits de création et de gestion des buckets (par exemple `roles/storage.admin`) sur le projet, et l’API Cloud Storage (`storage.googleapis.com`) doit être activée. Si nécessaire, un administrateur peut l’activer avec :

```sh
gcloud services enable storage.googleapis.com --project=industrial-intelligence-dev
```

`tofu apply` crée une ressource facturable. Aucun déploiement n’est effectué automatiquement. L’état OpenTofu reste local pour cette première configuration : conserver `terraform.tfstate` hors de Git et le sauvegarder ; le bucket de données n’est pas un backend d’état.
