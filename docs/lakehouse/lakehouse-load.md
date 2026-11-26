# Publication Silver → lakehouse Iceberg (`lakehouse-load`)

Le worker publie le **dernier run Silver complet** sous forme de tables **Apache Iceberg** (fichiers Parquet ZSTD et métadonnées Iceberg) dans le catalogue BigLake du projet. On obtient un format unique, ouvert, lisible par BigQuery, Spark ou DuckDB.

## Source et cible

| | |
|---|---|
| Entrée | `gs://industrial-intelligence-dev-data/silver/samples/<run>/` : un `_manifest.json` (`status`, tables, parts, sha256, génération) et un dossier Parquet par table |
| Catalogue | BigLake Iceberg REST `industrial_intelligence`, `warehouse = bl://projects/industrial-intelligence-dev/catalogs/industrial_intelligence` |
| Namespace | `silver`, données sous `gs://industrial-intelligence-dev-data/iceberg/silver/<table>/` |
| Lecture SQL | `` SELECT … FROM `industrial-intelligence-dev.industrial_intelligence.silver.entreprises_sirene` `` |

Le catalogue et le namespace existent en mode `CREDENTIAL_MODE_END_USER` : l'identité qui lit ou écrit doit avoir elle-même les droits GCS sur `iceberg/`. Cette configuration les référence sans les déclarer, mais d'anciennes déclarations restent dans l'état OpenTofu local : voir l'avertissement de déploiement ci-dessous.

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

**Déployé en dev le 19/11/2026**, activé dans `envs/dev.tfvars`, image du commit
`d97be7b` épinglée au digest
`sha256:5df15229681b2d51a3f027d0d03005400993f1ce87cc85d363e82fd5aac855f5`.

- Cloud Run Job `lakehouse-load` (`europe-west9`, 1 vCPU, 2 Gio, 1 h) et Scheduler `lakehouse-load-daily` à 12:00 UTC.
- Compte de service `lakehouse-load` : lecture de `silver/samples/` et `iceberg/` (plus le bucket lui-même, pour lister), création seule sous `iceberg/`, `roles/biglake.editor` et `roles/serviceusage.serviceUsageConsumer` sur le projet.

```sh
docker build --platform linux/amd64 -f Dockerfile.lakehouse -t europe-west9-docker.pkg.dev/industrial-intelligence-dev/rne/lakehouse:<commit> .
docker push europe-west9-docker.pkg.dev/industrial-intelligence-dev/rne/lakehouse:<commit>
# dev.tfvars : enable_lakehouse_load = true, lakehouse_image = "…/lakehouse@sha256:<digest>"
# Seulement après réconciliation de la configuration et de l'état local :
tofu plan -var-file=envs/dev.tfvars -var-file=envs/rne-dev.tfvars -out=lakehouse.tfplan
```

### Avertissement sur l'état local — 19/11/2026

Le plan global avec les deux tfvars propose **19 suppressions**, notamment le
catalogue/namespace BigLake, des API, l'ancien environnement Spark et le job
Silver, car leurs déclarations ne sont pas dans ce checkout. **Ce plan n'a pas
été appliqué. Ne pas appliquer un plan global avant réconciliation du code et
de l'état. Ne pas supprimer ces ressources ni retirer des entrées de l'état
sans décision explicite.** Le prototype Silver précédent est conservé dans le
stash de la branche `feat/silver-pipeline`.

Pour ce déploiement seulement, après sauvegarde de `terraform.tfstate`, un plan
exceptionnel ciblé sur la nouvelle chaîne a été relu et appliqué : **9 créations,
0 modification, 0 destruction**. Le ciblage n'est pas la procédure normale de
maintenance et ne résout pas la divergence restante.

```sh
cd infra/opentofu
tofu plan -var-file=envs/dev.tfvars -var-file=envs/rne-dev.tfvars \
  -target='google_cloud_scheduler_job.lakehouse_load[0]' -out=lakehouse-safe.tfplan
# Sauvegarder l'état et relire le plan avant tout apply approuvé.
```

### Validation depuis Cloud Run — 19/11/2026

Entrée figée : run Silver `20261119T103930Z-658dfffa4f5c`, 9 tables,
14 074 lignes, 884 065 octets de Parquet source. Aucun traitement national.

- Premier lancement `lakehouse-load-qtxrf` : `ForbiddenError` immédiatement après
  création des droits. Une sonde sous la même identité, puis les exécutions
  suivantes ont réussi sans modification du code ou des rôles. Un délai de
  propagation IAM est plausible, mais la cause exacte n'est pas prouvée.
- Lancement normal `lakehouse-load-zjxkh` : 9 tables déjà publiées, 9 sautées.
- Test d'écriture `lakehouse-load-b87xd` dans le namespace neuf
  `silver_validation_20261119_114819` : **9 tables publiées, 14 074 lignes**.
  Le namespace avait été préparé avec la propriété
  `location=gs://industrial-intelligence-dev-data/iceberg/silver_validation_20261119_114819`.
  La lecture Parquet et les écritures Iceberg ont été effectuées par le compte
  de service du job, pas par le compte utilisateur.
- Relecture via Iceberg des neuf tables : nombres de lignes, types chaînes et
  longueurs SIREN/SIRET vérifiés. Les snapshots du namespace `silver` sont restés
  identiques à ceux relevés avant l'essai.
- Réexécution `lakehouse-load-pbg75` dans le namespace de validation : 9 tables
  sautées, aucun nouveau snapshot.
- Lecture BigQuery des neuf tables de validation : **14 074 lignes**, requête
  plafonnée à 100 Mio facturés.
- Scheduler réactivé, `ENABLED`, quotidien à **12:00 UTC**. Déclenchement manuel
  via Scheduler vérifié : `lakehouse-load-j5v5j`, succès, 9 tables sautées.

Le job temporaire de diagnostic a été supprimé. Le namespace de validation et
ses données restent présents pour inspection ; aucune suppression de données
n'a été effectuée. Le job permanent garde `LAKEHOUSE_NAMESPACE=silver` : les
arguments du namespace de validation ne concernaient que les deux exécutions.

## Tests et limites

- `uv run pytest tests/test_lakehouse_load.py -q` : catalogue PyIceberg SQLite local, sans réseau ni GCP.
- Le worker suppose des schémas Silver stables ou additifs : un changement de type fait échouer la transaction, bruyamment.
- Seul le dernier run complet est publié. Un run plus ancien qu'on voudrait republier exige un appel manuel.
