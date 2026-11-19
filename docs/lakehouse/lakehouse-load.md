# Publication Silver → lakehouse Iceberg (`lakehouse-load`)

Le worker publie le **dernier run Silver complet** sous forme de tables **Apache Iceberg** (fichiers Parquet ZSTD et métadonnées Iceberg) dans le catalogue BigLake du projet. On obtient un format unique, ouvert, lisible par BigQuery, Spark ou DuckDB.

## Source et cible

| | |
|---|---|
| Entrée | `gs://industrial-intelligence-dev-data/silver/samples/<run>/` : un `_manifest.json` (`status`, tables, parts, sha256, génération) et un dossier Parquet par table |
| Catalogue | BigLake Iceberg REST `industrial_intelligence`, `warehouse = bl://projects/industrial-intelligence-dev/catalogs/industrial_intelligence` |
| Namespace | `silver`, données sous `gs://industrial-intelligence-dev-data/iceberg/silver/<table>/` |
| Lecture SQL | `` SELECT … FROM `industrial-intelligence-dev.industrial_intelligence.silver.entreprises_sirene` `` |

Le catalogue et le namespace ont été créés le 29/10/2026, hors OpenTofu, en mode `CREDENTIAL_MODE_END_USER` : l'identité qui lit ou écrit doit avoir elle-même les droits GCS sur `iceberg/`.

## Fonctionnement

1. Lister `silver/samples/*/_manifest.json` et retenir le run `status = complete` le plus récent. Les dossiers commencent par un horodatage UTC ; un run en cours est ignoré.
2. Valider le manifeste : `schema_version = 1`, parts dans le dossier du run, génération et nombre de lignes présents, total cohérent.
3. Pour chaque table, en **une transaction Iceberg** : union du schéma par nom, suppression des lignes du run précédent, ajout des lots Parquet triés. Les sources sont lues à la **génération GCS du manifeste**, par lots de 500 000 lignes.
4. Le snapshot porte `silver-run` et `silver-input-fingerprint`. Une table dont le snapshot courant porte déjà le run est **sautée** : relancer le worker ne fait rien.

Si le nombre de lignes lues ne correspond pas au manifeste, ou en cas d'erreur, la transaction n'est pas commitée et les lecteurs continuent de voir le run précédent. Les runs précédents restent lisibles par *time travel* Iceberg.

**Essai réel du 19/11/2026**, lancé depuis un poste avec un jeton utilisateur et non depuis le job : run `20261119T103930Z-658dfffa4f5c`, 9 tables publiées, 14 074 lignes (égal au manifeste), environ 1,1 Mo sous `iceberg/silver/`. Une deuxième exécution saute les 9 tables. Lecture BigQuery vérifiée, avec des `siren` de 9 caractères.

## Optimisations

- **Tri** sur la clé de jointure (`siren`, `siret`, `dateparution` pour BODACC), déclaré comme *sort order* Iceberg. Les statistiques min/max des fichiers Parquet permettent de sauter les fichiers qui ne contiennent pas la clé ou la plage de dates filtrée.
- **Pas de partitionnement pour l'instant** : PyIceberg exige l'extension native `pyiceberg-core` pour écrire des partitions, et des tables de la taille d'un sample n'y gagnent rien. À revoir pour le BODACC national (`month(dateparution)`).
- **Pas de compaction ni d'expiration de snapshots** : chaque run ajoute un snapshot et des fichiers. C'est négligeable sur les samples, à planifier au passage aux tables nationales.

## Exécution

```sh
gcloud auth application-default login   # ADC utilisateur, jamais de clé JSON
uv run lakehouse-load --bucket industrial-intelligence-dev-data \
  --project industrial-intelligence-dev --catalog industrial_intelligence
```

Options : `--prefix` (`silver/samples` par défaut), `--namespace` (`silver`). Toutes ont un équivalent `LAKEHOUSE_*` utilisé par le job Cloud Run.

Logs : `table_published` ou `table_skipped` (table, lignes), puis `command_completed` (`run`, `tables_published`, `tables_skipped`, `rows`).

## Déploiement (`infra/opentofu/lakehouse.tf`, désactivé par défaut)

- Cloud Run Job `lakehouse-load` (`europe-west9`, 1 vCPU, 2 Gio, 1 h) et Scheduler `lakehouse-load-daily` à 12:00 UTC.
- Compte de service `lakehouse-load` : lecture de `silver/samples/` et `iceberg/` (plus le bucket lui-même, pour lister), création seule sous `iceberg/`, `roles/biglake.editor` et `roles/serviceusage.serviceUsageConsumer` sur le projet.

```sh
docker build --platform linux/amd64 -f Dockerfile.lakehouse -t europe-west9-docker.pkg.dev/industrial-intelligence-dev/rne/lakehouse:<commit> .
docker push europe-west9-docker.pkg.dev/industrial-intelligence-dev/rne/lakehouse:<commit>
# dev.tfvars : enable_lakehouse_load = true, lakehouse_image = "…/lakehouse@sha256:<digest>"
tofu plan -var-file=envs/dev.tfvars -var-file=envs/rne-dev.tfvars -out=lakehouse.tfplan
```

## Tests et limites

- `uv run pytest tests/test_lakehouse_load.py -q` : catalogue PyIceberg SQLite local, sans réseau ni GCP.
- Le worker suppose des schémas Silver stables ou additifs : un changement de type fait échouer la transaction, bruyamment.
- Seul le dernier run complet est publié. Un run plus ancien qu'on voudrait republier exige un appel manuel.
