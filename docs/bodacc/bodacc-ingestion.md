# Ingestion BODACC (DILA)

Copie quotidienne des annonces commerciales du BODACC vers `raw/`, un export Parquet par jour de parution, depuis 2008.

## Source

- API OpenDataSoft de la DILA : `https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/annonces-commerciales`. Publique, gratuite, sans authentification. Fiche officielle : [`DILA_API-BODACC_Presentation_20260327.pdf`](https://echanges.dila.gouv.fr/OPENDATA/BODACC/DILA_API-BODACC_Presentation_20260327.pdf).
- Contenu vérifié le 29/10/2026 : 51,2 M annonces du 02/01/2008 à la veille, 2,1 à 3,8 M par an, de 0 à ~32 000 par jour de parution. Bulletins `A` (ventes et cessions, immatriculations, procédures collectives), `B` (modifications, radiations), `C` (dépôts des comptes).
- La fiche DILA annonce une mise à jour hebdomadaire ; en pratique, les annonces du jour sont présentes le jour même (traitement vers 7 h UTC).
- Les archives XML de la DILA (`echanges.dila.gouv.fr/OPENDATA/BODACC/`) sont la source d'origine de l'API ; elles ne sont pas utilisées tant qu'aucun champ ne manque dans l'API.
- Limites constatées : la pagination `/records` plafonne à 100 lignes par appel et 10 000 au total ; l'export `/exports/parquet` n'a pas cette limite. Quota affiché : 5 M d'appels par jour.

## Exécution

```sh
# Essai : 3 jours de janvier 2008 sous un préfixe d'essai (checkpoint séparé du job)
uv run bodacc-ingest --bucket industrial-intelligence-dev-data --prefix sandbox/bodacc --until 2008-01-03

# Comportement du job : de 2008 (ou du checkpoint) jusqu'à la veille
uv run bodacc-ingest --bucket industrial-intelligence-dev-data
```

`--bucket` et `--prefix` peuvent venir de `BODACC_BUCKET` et `BODACC_PREFIX` (défaut `dila/bodacc`). Autres options : `--until` (défaut : la veille, jamais le jour même), `--recheck-days` (défaut 30), `--max-seconds` (défaut 20 000). Code de sortie : `0` en cas de succès, y compris quand le budget de temps arrête le run (`status: partial`), `1` en cas d'échec.

Tests : `uv run pytest tests/test_bodacc_archive.py -q` (API simulée par `httpx.MockTransport`, stockage en mémoire).

## Sur GCP

`infra/opentofu/bodacc.tf`, désactivé par défaut (`enable_bodacc_ingestion`) :

- Cloud Run Job `bodacc-archive` (`europe-west9`, 1 vCPU, 1 Gio, timeout 6 h, budget de 20 000 s).
- Compte de service `bodacc-archive` : création et lecture sous `raw/dila/bodacc/`, lecture-écriture sous `control/dila/bodacc/` (checkpoint). Il ne peut ni écraser ni supprimer un export.
- Scheduler `bodacc-archive-daily` à 08:00 UTC.
- Image `Dockerfile.bodacc`, publiée dans le dépôt Artifact Registry `rne` sous le nom `bodacc` et épinglée par digest dans `bodacc_image`.

```sh
docker build --platform linux/amd64 -f Dockerfile.bodacc -t europe-west9-docker.pkg.dev/industrial-intelligence-dev/rne/bodacc:<tag> .
docker push europe-west9-docker.pkg.dev/industrial-intelligence-dev/rne/bodacc:<tag>
# Dans envs/dev.tfvars : enable_bodacc_ingestion = true et bodacc_image = "…@sha256:…", puis plan/apply habituels
gcloud run jobs execute bodacc-archive --region europe-west9   # run manuel
```

Premier chargement : environ 6 900 jours de 2008 à aujourd'hui et **12 à 13 Go** (extrapolé de 1,3 Mo pour 5 928 annonces). Mesuré le 29/10/2026 : ~2,9 s par jour, soit plus que le budget de 5,5 h d'un run ; le run s'arrête en `partial` et le suivant reprend au checkpoint. Coût estimé : stockage ~0,35 $/mois, Cloud Run < 1 $ pour le rattrapage.

## Fonctionnement

1. Lire `control/<prefix>/checkpoint.json` (`completed_until`, dernier jour archivé). Sans checkpoint, partir du 01/01/2008.
2. Pour chaque jour, des `recheck_days` derniers jours archivés jusqu'à la veille :
   - lire le nombre d'annonces du jour (`/records?limit=0`) ;
   - exporter le jour en Parquet, **trié par `id`** : un contenu identique donne des octets identiques (vérifié sur l'API réelle) ;
   - valider : nombre de lignes égal au compte, `id` présents et uniques, une seule `dateparution`, les 32 colonnes attendues présentes ;
   - nommer le fichier par son SHA-256. Un export déjà archivé est sauté ; un export différent d'un jour déjà archivé est ajouté à côté (événement `day_changed`, avec `age_days`).
3. Avancer le checkpoint après chaque nouveau jour stocké. Réessais sur 429, 5xx et erreurs réseau, en respectant `Retry-After`.

Les jours sans parution (week-ends, fériés) sont archivés comme les autres : un Parquet de 0 ligne avec le schéma complet.

## Schéma de sortie

```text
raw/<prefix>/annonces/dateparution=2026-10-28/
  <sha256>.parquet                 # export tel que renvoyé par l'API
  <sha256>.parquet.manifest.json   # provenance : url, where, order_by, rows, size, sha256, generation, retrieved_at
control/<prefix>/checkpoint.json   # {"schema_version": 1, "completed_until": "2026-10-28"}
```

- Lecteurs : ne lire que les Parquet qui ont un manifeste et, pour un jour qui en a plusieurs, ne garder que la version au `retrieved_at` le plus récent. Un glob `*.parquet` compterait deux fois un jour republié.
- Colonnes : 32, dont `id` (`A202602082224` = bulletin A, parution 208, annonce 2224), `dateparution`, `familleavis_lib`, `typeavis_lib` (initial, rectificatif, annulation), `registre` (le ou les SIREN, sous forme de texte), `tribunal`, `cp`, `ville`.
- `listepersonnes`, `listeetablissements`, `jugement`, `acte`, `modificationsgenerales`, `radiationaurcs`, `depot`, `listeprecedentproprietaire`, `listeprecedentexploitant` sont des **objets JSON encodés en texte** : leur parsing revient à l'ETL. Exemples : procédure collective et date du jugement, vendeur et acquéreur d'un fonds avec leurs SIREN et le prix, date de clôture des comptes déposés.

## Limites

- **Fenêtre de 30 jours non vérifiée.** La DILA retraite ses données (les archives annuelles sont republiées), mais on ne sait pas jusqu'à quelle ancienneté un jour change. Mesurer avec les logs, puis ajuster `--recheck-days` :
  ```sh
  gcloud logging read 'resource.labels.job_name="bodacc-archive" AND jsonPayload.event="day_changed"' \
    --project industrial-intelligence-dev --freshness 30d --format="value(jsonPayload.day,jsonPayload.age_days)"
  ```
  Un changement plus ancien que la fenêtre n'est pas récupéré.
- Si le compte de l'API bouge pendant l'export (retraitement en cours), le run échoue sans rien stocker pour ce jour ; le run suivant le reprend.
- `listepersonnes` contient des noms de personnes physiques. Lire l'avertissement de la DILA sur la réutilisation des données personnelles avant de les exposer.
- Seul le jeu `annonces-commerciales` est collecté, pas les annonces civiles.
