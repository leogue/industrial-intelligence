# Ingestion SIRENE (data.gouv.fr)

Copie mensuelle des 6 stocks SIRENE au format Parquet, publiés par l'INSEE sur data.gouv.fr, vers GCS. Exploration de la source : [`sirene.md`](sirene.md).

## Source

- Catalogue : `GET https://www.data.gouv.fr/api/1/datasets/5b7ffc618b4c4169d30727e0/`. Public, sans authentification, Licence Ouverte v2.0.
- Publication mensuelle, au 1er du mois. data.gouv.fr **remplace** le fichier sous le même `resource_id` : il ne garde que le dernier mois. L'historique des snapshots n'existe donc que si on archive chaque mois.
- Chaque ressource fournit `url`, `filesize`, `last_modified` et un `checksum` SHA-1. Vérifié le 28/10/2026 : le SHA-1 correspond au fichier téléchargé. Les en-têtes HTTP du CDN (`Last-Modified`, `ETag`) ne sont pas fiables pour détecter une nouvelle publication.
- Pas de mise à jour quotidienne dans ce jeu de données. Pour du quotidien, il faudrait l'API Sirene de l'INSEE (avec une clé).

| Table | `resource_id` | Taille au 01/10/2026 |
|---|---|---|
| `unite_legale` | `350182c9-148a-46e0-8389-76c2ec1374a3` | 714 Mo |
| `etablissement` | `a29c1297-1f92-4e2a-8f6b-8c902ce96c5f` | 2,22 Go |
| `unite_legale_historique` | `1b9290ed-d0bc-461f-ba31-0250a99cc140` | 863 Mo |
| `etablissement_historique` | `2b3a0c79-f97b-46b8-ac02-8be6c1f01a8c` | 878 Mo |
| `etablissement_liens_succession` | `6ea8c0d2-a4b9-4ea2-ba63-7ec89c2dd4f4` | 119 Mo |
| `doublons` | `ec0a0d5f-5d52-4603-a712-b6821076abf0` | 1 Mo |

Total : environ 4,8 Go par mois.

## Exécution

```sh
# Essai : 1 Mo
uv run sirene-ingest --bucket industrial-intelligence-dev-data --table doublons

# Les 6 tables (≈ 4,8 Go) : nécessite --allow-large-download
uv run sirene-ingest --bucket industrial-intelligence-dev-data --allow-large-download
```

Le préfixe par défaut est `insee/sirene`, comme `inpi/rne` pour le RNE (pas de préfixe personnel). `--bucket` et `--prefix` peuvent aussi venir de `SIRENE_BUCKET` et `SIRENE_PREFIX`. Code de sortie : `0` en cas de succès, `1` en cas d'échec.

## Sur GCP

`infra/opentofu/sirene.tf`, activé dans `envs/dev.tfvars` :

- Cloud Run Job `sirene-archive` (`europe-west9`, 1 vCPU, 1 Gio, timeout 6 h, `--allow-large-download`). Le transfert se fait depuis GCP, pas depuis un poste.
- Compte de service `sirene-archive` : création et lecture seulement sous `bronze/insee/sirene/`. Il ne peut ni écraser ni supprimer.
- Scheduler `sirene-archive-daily` à 06:00 UTC. Un run déjà à jour ne fait qu'un appel au catalogue. Le run quotidien récupère donc la publication du mois dès sa sortie et rattrape seul un échec.
- Image `Dockerfile.sirene`, publiée dans le dépôt Artifact Registry `rne` sous le nom `sirene` et épinglée par digest dans `sirene_image`.

```sh
docker build --platform linux/amd64 -f Dockerfile.sirene -t europe-west9-docker.pkg.dev/industrial-intelligence-dev/rne/sirene:<tag> .
docker push europe-west9-docker.pkg.dev/industrial-intelligence-dev/rne/sirene:<tag>
# Reporter le digest dans envs/dev.tfvars (sirene_image), puis plan/apply habituels
gcloud run jobs execute sirene-archive --region europe-west9   # run manuel
```

## Fonctionnement

1. Lire le catalogue, avec des réessais sur 429, 5xx et les erreurs réseau (en respectant `Retry-After`). Valider chaque ressource demandée : format `parquet`, SHA-1, taille, URL `static.data.gouv.fr`. Le job échoue si une ressource attendue disparaît.
2. Si `_manifest.json` existe déjà pour cette table et cette date avec le même SHA-1, sauter la table. Si le SHA-1 diffère, le job échoue : on n'écrase jamais.
3. Sinon, télécharger en flux vers GCS en calculant au passage le SHA-1 et le SHA-256. L'upload GCS n'est finalisé que si la taille et le SHA-1 correspondent à ceux annoncés. Au-delà de 100 Mio par fichier, `--allow-large-download` est obligatoire.
4. Écrire `_manifest.json` en dernier : c'est le marqueur de succès.

Un téléchargement interrompu ne laisse rien dans GCS ; il suffit de relancer. Seule la lecture du catalogue est réessayée : le téléchargement d'un fichier ne l'est pas dans le même run, et les tables déjà archivées sont sautées à la relance.

## Schéma de sortie

```text
bronze/<prefix>/<table>/
  date=2026-10-01/
    stock-stockunitelegale-parquet.parquet   # original data.gouv.fr, non modifié
    _manifest.json
```

- `date` est la date UTC de `last_modified` de la ressource data.gouv.fr.
- Le Parquet est l'original publié (écrit par DuckDB) : `siren` et `siret` y sont des chaînes. Comme la source est déjà en Parquet, l'original est rangé directement en `bronze/`, sans copie `raw/`.
- `_manifest.json` contient la provenance : `remote` (`resource_id`, `url`, `title`, `size`, `sha1`, `last_modified`), `retrieved_at` UTC, `object`, `uri`, `size`, `sha1`, `sha256` et `generation` GCS.
- Lecteurs : ne lire une partition `date=…` que si son `_manifest.json` existe.

## Limites

- Si le job s'arrête entre la finalisation du Parquet et l'écriture du manifest (fenêtre très courte), la relance échoue avec `Output object already exists`. Il faut alors vérifier l'objet à la main avant de le supprimer.
