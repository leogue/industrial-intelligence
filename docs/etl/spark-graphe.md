# ETL Spark : du bronze au graphe d'entreprises (`etl/graph.py`)

Un batch **Serverless for Apache Spark** (Dataproc, runtime 4.0 : Spark 4.2, Python 3.13) lit
les archives SIRENE, RNE et BODACC déjà présentes dans GCS, relie les entités par `siren` et
`siret`, puis écrit des tables **Iceberg** dans le catalogue BigLake `industrial_intelligence`.
BigQuery, Spark ou DuckDB les lisent ensuite directement.

Aucun appel aux API INPI, data.gouv.fr ou DILA. Les originaux `raw/` et `bronze/` ne sont que lus.

**État au 26/11/2026** : namespaces `silver_spark` et `gold` créés, premier batch Dataproc
**réussi en échantillon** (`--siren-prefix 30`, voir [premier batch](#premier-batch-dataproc--26112026)).
Aucun calcul national ni Scheduler.

## Entrées

| Source | Lecture | Version retenue |
|---|---|---|
| SIRENE `unite_legale`, `etablissement`, `etablissement_liens_succession`, `doublons` | `bronze/insee/sirene/<table>/date=…/*.parquet` | Dernière partition `date=` de chaque table |
| RNE stock | `bronze/inpi/rne/companies/stock/<snapshot>/part-*.parquet` | Snapshot `--rne-stock`, exige `_manifest.json` `status = complete` |
| RNE diff | `bronze/inpi/rne/companies/diff/from=…/to=…/companies.parquet` | Fenêtres dont `to` ≤ `completed_until` de `control/inpi/rne/diff/checkpoint.json` |
| BODACC | `raw/dila/bodacc/annonces/dateparution=…/*.parquet` | Jours ≤ `completed_until` de `control/dila/bodacc/checkpoint.json` ; par `id`, le fichier le plus récent |

Fiche RNE courante par SIREN : `source_updated_at` le plus récent, puis `source_retrieved_at`,
le diff avant le stock à égalité, puis `company_sha256`. C'est l'ordre du prototype Silver. Le
JSON est parsé **avant** le tri, avec un schéma partiel : seuls les champs utiles quittent le
document complet.

Contrôles bloquants avant toute écriture : SIREN sur 9 chiffres, SIRET sur 14 chiffres, et
`left(siret, 9) = siren`.

## Tables produites

### Silver (`industrial_intelligence.silver_spark`)

| Table | Une ligne = | Clé de tri |
|---|---|---|
| `entreprises` | un SIREN connu de SIRENE **ou** du RNE ; colonnes SIRENE et `rne_*` côte à côte, adresse du siège, nombre d'établissements, `dans_sirene` / `dans_rne` | `siren` |
| `etablissements` | un SIRET (SIRENE), avec adresse, commune, département, NAF, effectifs, état | `siren, siret` |
| `dirigeants` | un pouvoir RNE (`composition.pouvoirs[]`) ou un entrepreneur individuel ; personne physique (nom, prénoms, mois de naissance) ou morale (`siren_dirigeant`) | `siren` |
| `annonces_bodacc` | une annonce, avec `sirens` (lus dans `registre`), `sirens_cedants`, jugement et dépôt des comptes | `dateparution, id` |
| `liens` | **une relation**, sous forme d'arête `(de_type, de_id) → (vers_type, vers_id)` | `de_id, vers_id` |

Les valeurs vides et `[ND]` de SIRENE deviennent nulles. Les dates métier restent telles que
fournies (chaînes pour le RNE).

### Types de liens (`silver_spark.liens`)

| `type_lien` | De → vers | `preuve` | Source |
|---|---|---|---|
| `mandat_personne_morale` | entreprise dirigeante → entreprise | `fait` | RNE, pouvoir avec `entreprise.siren` |
| `mandat_personne_physique` | personne → entreprise | `inference` | RNE ; la personne est identifiée par nom, prénoms et mois de naissance |
| `entrepreneur_individuel` | personne → entreprise | `inference` | RNE `personnePhysique` |
| `succession_etablissement` | établissement → établissement | `fait` | SIRENE, liens de succession |
| `succession_entre_entreprises` | entreprise → entreprise | `fait` | Agrégat des successions qui changent de SIREN |
| `cession_fonds` | cédant → acquéreur | `fait` | BODACC `vente` : précédent propriétaire → autres SIREN de l'annonce |
| `doublon_siren` | SIREN → SIREN | `fait` | SIRENE, doublons |

**Aucune arête n'est un lien de propriété.** Un mandat n'est pas une détention de capital, et une
cession de fonds de commerce transfère un actif, pas des parts. `personne_id` est un SHA-256 de
`NOM|PRÉNOMS|AAAA-MM` : deux homonymes nés le même mois sont confondus, d'où `preuve = inference`.
Sans mois de naissance, aucune identité n'est calculée.

Les codes `role_code` sont ceux de l'INPI (`roleEntreprise`). Leur table de libellés est dans le
dictionnaire de l'annexe 1 de la documentation API (voir `docs/rne/inpi.md`). Elle n'est pas
encore chargée : aucun libellé n'est deviné.

### Gold (`industrial_intelligence.gold`)

| Table | Usage |
|---|---|
| `fiche_entreprise` | La page entreprise en une ligne : identité, siège, capital, dirigeants actifs (tableau), nombre d'annonces BODACC, dernière procédure collective et son jugement, dernier exercice déposé |
| `reseau_entreprises` | Arêtes entreprise ↔ entreprise avec les deux dénominations : liens directs, plus `dirigeant_commun` (`inference`) et `personne_morale_commune` (`fait`) |
| `indicateurs_territoire` | Par mois × département × division NAF : créations SIRENE, créations BODACC, procédures collectives, radiations, ventes de fonds |

`reseau_entreprises` ignore les dirigeants ayant plus de 50 mandats (`MAX_SHARED_MANDATES`). Ce
sont des professionnels (commissaires aux comptes, gestionnaires), dont tous les couples
d'entreprises ajouteraient des millions d'arêtes sans rien dire sur les groupes.

### Optimisations

- Chaque table est répartie par plages (`repartitionByRange`) puis triée sur sa clé. Les
  statistiques min/max Parquet permettent alors de sauter presque tous les fichiers sur
  `WHERE siren = …`.
- Le JSON RNE (plusieurs Ko par fiche) est réduit à un schéma partiel avant le tri.
- Le gold relit les tables silver déjà commitées au lieu de recalculer leur lignage.
- Parquet ZSTD. Chaque table est remplacée en un seul commit Iceberg (`createOrReplace`) : les
  lecteurs voient l'ancien ou le nouveau run, jamais un mélange, et les runs précédents restent
  lisibles par *time travel*.
- Provenance : chaque snapshot porte `etl-run`, `etl-sample` et `etl-inputs`. Ce dernier est un
  JSON des partitions lues, du snapshot RNE et des bornes des checkpoints.

## Requêtes d'exemple (BigQuery)

```sql
-- Fiche entreprise (type societe.com / Pappers)
SELECT * FROM `industrial-intelligence-dev.industrial_intelligence.gold.fiche_entreprise`
WHERE siren = '005420120';

-- Réseau d'une entreprise : à qui elle est liée, et pourquoi
SELECT * FROM `industrial-intelligence-dev.industrial_intelligence.gold.reseau_entreprises`
WHERE '005420120' IN (siren_a, siren_b);

-- Les autres mandats des dirigeants d'une entreprise (identité inférée)
SELECT d.prenoms, d.nom, autre.siren, e.denomination, autre.role_code
FROM `industrial-intelligence-dev.industrial_intelligence.silver_spark.dirigeants` d
JOIN `industrial-intelligence-dev.industrial_intelligence.silver_spark.dirigeants` autre
  USING (personne_id)
JOIN `industrial-intelligence-dev.industrial_intelligence.silver_spark.entreprises` e
  ON e.siren = autre.siren
WHERE d.siren = '005420120' AND autre.siren <> d.siren;

-- Les personnes morales qui dirigent le plus de sociétés (holdings, groupes)
SELECT de_id AS siren_dirigeant, e.denomination, count(*) AS societes_dirigees
FROM `industrial-intelligence-dev.industrial_intelligence.silver_spark.liens` l
JOIN `industrial-intelligence-dev.industrial_intelligence.silver_spark.entreprises` e
  ON e.siren = l.de_id
WHERE type_lien = 'mandat_personne_morale'
GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 20;

-- Procédures collectives des 12 derniers mois par secteur dans un département
SELECT division_naf, sum(procedures_collectives) AS procedures, sum(creations_sirene) AS creations
FROM `industrial-intelligence-dev.industrial_intelligence.gold.indicateurs_territoire`
WHERE departement = '69' AND mois >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
GROUP BY 1 ORDER BY 2 DESC;

-- Entreprises actives employeuses avec une procédure collective récente
SELECT siren, denomination, tranche_effectifs, derniere_procedure_collective, dernier_jugement
FROM `industrial-intelligence-dev.industrial_intelligence.gold.fiche_entreprise`
WHERE etat_administratif = 'A' AND employeur = 'O'
  AND derniere_procedure_collective >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
ORDER BY tranche_effectifs DESC;
```

Plafonner les essais avec `--maximum_bytes_billed`, comme pour la validation de `lakehouse-load`.

## Exécution

### Tests locaux

PySpark est une dépendance de dev uniquement : Dataproc fournit déjà Spark. Les tests exigent un
JDK 17 ou plus récent. La CI GitHub (`ubuntu-latest`) en fournit un.

```sh
brew install openjdk@21                       # une fois
export JAVA_HOME="$(brew --prefix openjdk@21)"
uv run pytest tests/test_etl_graph.py -q
```

Le garde-fou réseau de `tests/conftest.py` autorise uniquement la boucle locale, que le driver
Python utilise pour parler à sa JVM.

**Essai local de bout en bout du 26/11/2026**, sur un row group réel de chaque table SIRENE, une
part du stock RNE (5 000 fiches), une fenêtre de diff et un jour de BODACC (23 423 annonces).
Le catalogue Iceberg était local, avec PySpark 4.1 et Iceberg 1.12 : Maven ne publie pas encore
d'`iceberg-spark-runtime` pour Spark 4.2. Les 8 tables sont écrites en environ 45 s : 132 881
entreprises, 124 782 établissements, 20 606 dirigeants, 300 051 liens. SIREN et SIRET sont
uniques, aucune arête n'a d'extrémité nulle, et les propriétés de snapshot sont présentes. Les
sources ne couvrant pas les mêmes entreprises dans ces extraits, les jointures y sont
volontairement partielles.

### Batch Dataproc (à faire approuver)

Prérequis : les namespaces créés par `infra/opentofu/spark.tf` (voir l'infrastructure ci-dessous).

```sh
COMMIT=$(git rev-parse --short HEAD)
gcloud storage cp src/industrial_intelligence/etl/graph.py \
  gs://industrial-intelligence-dev-data/code/etl/$COMMIT/graph.py

CATALOG=spark.sql.catalog.industrial_intelligence
gcloud dataproc batches submit pyspark \
  gs://industrial-intelligence-dev-data/code/etl/$COMMIT/graph.py \
  --project industrial-intelligence-dev --region europe-west9 --version 4.0 \
  --batch etl-graphe-$COMMIT-1 \
  --service-account etl-spark@industrial-intelligence-dev.iam.gserviceaccount.com \
  --subnet lakehouse-europe-west9 --ttl 4h \
  --properties="^#^$CATALOG=org.apache.iceberg.spark.SparkCatalog#$CATALOG.type=rest#$CATALOG.uri=https://biglake.googleapis.com/iceberg/v1/restcatalog#$CATALOG.warehouse=bl://projects/industrial-intelligence-dev/catalogs/industrial_intelligence#$CATALOG.header.x-goog-user-project=industrial-intelligence-dev#$CATALOG.rest.auth.type=org.apache.iceberg.gcp.auth.GoogleAuthManager#$CATALOG.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO#$CATALOG.rest-metrics-reporting-enabled=false#spark.dynamicAllocation.maxExecutors=10" \
  -- --root gs://industrial-intelligence-dev-data --siren-prefix 30
```

- `--siren-prefix 30` garde environ 1 % des SIREN dans **toutes** les sources, pour que les
  jointures restent cohérentes. Sans cette option, le calcul est national.
- `warehouse` doit être `bl://projects/…/catalogs/…`. L'essai du 29/10/2026
  (`sirene-unite-legale-948e54b-1`) a échoué avec `RESTException: Unable to process` dès
  `fetchConfig`, avec `warehouse = gs://…/iceberg`. C'est la cause probable, mais elle n'est pas
  prouvée. `lakehouse-load`, qui fonctionne, utilise bien `bl://`.
- Reste à vérifier au premier batch : que le runtime 4.0 embarque bien les classes Iceberg et
  `GoogleAuthManager`. La documentation du runtime ne cite pas la version d'Iceberg.

**Coût estimé, non mesuré.** Même en échantillon, le batch lit la colonne `company_json` de
presque tous les row groups RNE : les SIREN ne sont pas triés dans le stock. Il faut donc compter
environ 40 Go lus sur ~61 Go de bronze. Ordre de grandeur : quelques dizaines de DCU-heures pour
le national (de l'ordre de 5 à 10 €), un peu moins pour l'échantillon. `maxExecutors=10` borne
la dépense. À mesurer au premier batch avant tout lancement national ou toute planification.

### Premier batch Dataproc — 26/11/2026

`etl-graphe-62e3f1d-1`, code `gs://industrial-intelligence-dev-data/code/etl/62e3f1d/graph.py`,
runtime 4.0, `--siren-prefix 30`, `maxExecutors=10`. **Succès** : de 14:56 à 16:04 UTC, soit
**68 min**.

- La connexion au catalogue avec `warehouse = bl://…` fonctionne. Le runtime 4.0 fournit bien
  les classes Iceberg, `GoogleAuthManager` et `GCSFileIO`.
- Consommation : **6,15 DCU-heures** et **503 Go-heures** de stockage de shuffle, soit environ
  0,5 € selon la grille publique. C'est une estimation : la facture réelle n'a pas été relevée.
- Sorties : 189 Mo sous `iceberg/silver_spark/` et 33 Mo sous `iceberg/gold/`.

| Table | Lignes |
|---|---|
| `silver_spark.entreprises` | 529 136 |
| `silver_spark.etablissements` | 700 879 |
| `silver_spark.dirigeants` | 468 396 |
| `silver_spark.annonces_bodacc` | 4 233 695 |
| `silver_spark.liens` | 477 289 |
| `gold.fiche_entreprise` | 529 136 |
| `gold.reseau_entreprises` | 229 442 |
| `gold.indicateurs_territoire` | 180 627 |

Liens : 171 794 `succession_etablissement`, 113 262 `cession_fonds`, 89 374
`succession_entre_entreprises`, 51 920 `mandat_personne_physique`, 35 241
`entrepreneur_individuel`, 14 510 `mandat_personne_morale` et 1 188 `doublon_siren`.

Constats, à traiter avant le calcul national :

- **Des milliers de petits fichiers.** Environ 6 900 jours BODACC, 4 100 fenêtres de diff RNE
  et 5 470 parts de stock : environ 200 ouvertures de fichiers par minute.
  `annonces_bodacc` a pris environ 19 min et `liens` environ 13 min. Le filtre d'échantillon
  n'évite aucune lecture.
- **BODACC relu deux fois** : `liens` recalcule les annonces au lieu de relire la table écrite.
- **Filtre d'échantillon BODACC trop large** : `registre` *contient* le préfixe, au lieu de
  SIREN qui *commencent* par lui. D'où 4,2 M d'annonces et 113 k cessions de fonds. Les
  jointures restent exactes, puisqu'elles se font par SIREN.
- **Parallélisme faible** : environ 5 DCU en moyenne, alors que 10 executors sont autorisés.
  Cause non analysée.

## Infrastructure (`infra/opentofu/spark.tf`)

Ce fichier redéclare, **avec les mêmes adresses**, les ressources créées le 29/10/2026 depuis la
branche `feat/lakehouse-spark-iceberg` et restées dans l'état local : API Compute, Dataproc et
BigLake, réseau `lakehouse` (accès privé Google, sans NAT ni Internet), compte `etl-spark`,
catalogue et namespace `silver`. Il ajoute les namespaces `silver_spark` et `gold`, gérés par
OpenTofu : le job ne crée aucun namespace.

Plan du 26/11/2026 (`-var-file=envs/dev.tfvars -var-file=envs/rne-dev.tfvars`) :
**2 créations** (les namespaces), **0 modification**, et **5 destructions**. Ces dernières
concernent le job `silver-build` du prototype DuckDB, dont la configuration n'existe que dans un
stash. Les ressources Spark ne sont plus proposées à la destruction. Ne pas appliquer ce plan
global tel quel : soit restaurer `silver.tf`, soit décider explicitement de retirer
`silver-build`.

**Appliqué le 26/11/2026** en plan ciblé : 2 créations, sauvegarde
`terraform.tfstate.before-etl-namespaces-20261126T140224Z`. L'apply a créé les deux namespaces
dans BigLake, puis s'est terminé en erreur avant de les inscrire dans l'état. Ils ont ensuite été
ajoutés à l'état par `tofu import` (`google_biglake_iceberg_namespace.etl["gold"]` et
`["silver_spark"]`), sans autre modification. Commande utilisée :

```sh
cd infra/opentofu
tofu plan -var-file=envs/dev.tfvars -var-file=envs/rne-dev.tfvars \
  -target='google_biglake_iceberg_namespace.etl' -out=etl-namespaces.tfplan
# Sauvegarder terraform.tfstate, relire le plan, obtenir l'accord, puis tofu apply etl-namespaces.tfplan
```

## Limites

- Recalcul complet à chaque run, sans mise à jour incrémentale. C'est acceptable à ce volume et
  à une fréquence quotidienne au plus.
- Pas de Scheduler : à ajouter après mesure du coût d'un run national.
- Les pouvoirs RNE sont ceux de la **fiche courante** : les mandats passés (`historique[]`) ne
  sont pas encore exploités.
- Les bilans (`bilansSaisis`), les actes et les bénéficiaires effectifs ne sont pas intégrés. Le
  registre des bénéficiaires effectifs n'est pas en open data.
- Les sorties contiennent des données personnelles (dirigeants, entrepreneurs individuels) et
  les indicateurs de diffusion des sources (`statut_diffusion`, `rne_diffusion_commerciale`).
  Elles restent privées : leur existence n'autorise pas à republier les personnes ou entreprises
  non diffusibles.
- `lakehouse-load` continue de publier les samples DuckDB dans `silver`. Il faudra le retirer une
  fois cet ETL validé, ce qui demande une décision explicite.
