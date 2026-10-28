# Ingestion RNE — stock FTP + différentiel quotidien

**État : déployé en dev le 28 octobre 2026** (image du commit `deac8a8`, Scheduler toutes les 6 h). Le rattrapage depuis le 1er mars est en cours. Voir [le journal](deploiement-rne-dev.md#28-octobre--sync-simplifié-déployé).

## Le principe

```text
FTP INPI : stock_RNE_formalites_NIVEAU1_20260304_1400.zip  (photo du registre ≈ mars 2026)
   │ bootstrap     → raw/{prefix}/stocks/.../*.zip                      (fait, 15 Go)
   │ materialize   → bronze/{prefix}/companies/stock/{nom_du_zip}/*.parquet (27,3 M lignes)
   ▼
API /companies/diff : fiches modifiées depuis le 1er mars 2026
   │ sync (chaque jour) → raw/{prefix}/companies/diff/from=…/to=…/{sha256}.json
   │                    → bronze/{prefix}/companies/diff/from=…/to=…/companies.parquet
   ▼
État courant d'une entreprise = sa ligne la plus récente, stock ∪ diff (ETL Silver, à écrire)
```

- Le **stock** fournit toutes les entreprises et leur bloc `historique[]` (les événements depuis 2023 y sont déjà).
- Le **différentiel** renvoie la **fiche actuelle** des entreprises modifiées dans une fenêtre. Il ne donne pas l'état passé. Rejouer le diff depuis 2023 reviendrait à retélécharger tout le registre via l'API (le RNE a été entièrement migré début 2023) : c'est inutile.
- Il reste donc **un seul trou à combler** : de la date du stock jusqu'à aujourd'hui. Ensuite, il suffit de collecter chaque jour la veille.

La borne `2026-03-01T00:00:00Z` est une **hypothèse d'équipe**, pas une couverture certifiée par l'INPI. 5 469 des 5 470 fichiers du ZIP sont datés du 5 mars 2026, et on prend quelques jours de marge en amont ([détails](inpi.md#recherche-publique-complémentaire-et-datation-interne-du-zip)).

## Le code

```text
src/industrial_intelligence/ingestion/rne/
├── cli.py          # 3 commandes : bootstrap, materialize, sync
├── sources.py      # client API RNE (login, retries, 429) et client FTP du stock
├── storage.py      # GCS via ADC : objets immuables, checkpoint en compare-and-swap
├── companies.py    # schéma Parquet Bronze commun au stock et au diff
├── bootstrap.py    # FTP → GCS (ZIP original + reçu)
├── materialize.py  # ZIP GCS → Parquet Bronze, reprise par fichier
├── diff.py         # sync : rattrapage puis quotidien, un seul checkpoint
├── log.py          # logs JSON structurés pour Cloud Logging
└── common.py       # dates UTC, validation SIREN, erreurs sans secrets
```

### `sync` en une boucle (`diff.py`)

Checkpoint `control/{prefix}/diff/checkpoint.json` :

```json
{"schema_version": 1, "start": "2026-03-01T00:00:00.000000Z",
 "completed_until": "2026-03-04T18:00:00.000000Z",
 "window": {"from": "…", "to": "…", "expected": 7421, "records": 3100, "cursor": "…", "pages": [...]}}
```

1. **Ouvrir une fenêtre** `[completed_until, to]` : `/companies/diff/count` doit être exact (≤ 10 000). Sinon on coupe la fenêtre en deux. On démarre avec 6 h ; après chaque succès, la fenêtre double, jusqu'à 1 jour au maximum.
2. **Paginer** `/companies/diff` : chaque page JSON brute est écrite dans `raw/`, sous un nom égal à son SHA-256 (jamais écrasée). Le curseur est enregistré dans le checkpoint après chaque page.
3. **Clore** la fenêtre sur la page vide : le total doit être égal au count. Si c'est le cas, on écrit **un Parquet par fenêtre** et `completed_until` avance. Sinon, la fenêtre est remise à zéro et l'exécution échoue, de façon visible ; elle sera recollectée à l'exécution suivante.

Le premier lancement crée le checkpoint à `--start`. Les lancements suivants reprennent là où le précédent s'est arrêté (un `--start` différent est refusé). Quand le budget d'appels ou de temps est atteint, ou en cas de quota INPI (HTTP 429), l'exécution se termine **avec succès** et `status: "partial"` : la suivante continue. Une fois le retard rattrapé, chaque exécution ne collecte plus que la veille (`--lag-days 1` : jusqu'à minuit UTC moins un jour).

Pas de verrou ni de catalogue : le checkpoint est écrit en compare-and-swap (deux exécutions concurrentes → la seconde échoue sans rien écraser). Les pages brutes sont adressées par contenu et le Parquet a un chemin déterministe, donc rejouer après un crash ne crée pas de doublon.

### Sortie

Le schéma Bronze du diff est le même que celui du stock (`companies.COMPANY_SCHEMA`) : `siren`, quelques colonnes typées, le JSON complet exact dans `company_json` (décimaux préservés), `deleted` (tombstones) et la provenance par ligne (`source_uri`, `source_sha256`, `source_generation`, `source_retrieved_at`, `source_kind=diff`).

Une même entreprise peut apparaître dans plusieurs fenêtres : l'API inclut en pratique la borne basse, et une entreprise peut aussi être modifiée plusieurs fois. **La déduplication relève de l'ETL Silver**, pas de l'ingestion. Exemple de lecture de l'état courant (non implémenté, à valider) : garder par `siren` la ligne de plus grand `source_updated_at`, puis `source_retrieved_at`, sur stock ∪ diff, et marquer les `deleted = true`.

## Durée et coût estimés

Mesures sur le job actuel : **un jour de changements ≈ 1 650 appels API**, débit **≈ 1 900 appels/h** (exécution du 28/10/2026 : 2 154 appels en 68 min). Aucun HTTP 429 sur les exécutions du 24 au 28/10, mais aucune n'a dépassé ~2 600 appels/jour : **le quota réel du diff est inconnu**.

| | Estimation |
|---|---|
| Trou 01/03/2026 → fin octobre 2026 | ~240 jours ≈ **400 000 appels** ≈ 210 h de job |
| Une exécution (`--max-requests 10000`, `--max-seconds 20000` ≈ 5,5 h) | ~10 000 appels ≈ 6 jours de données |
| Scheduler **toutes les 6 h** (`0 */6 * * *`), sans quota bloquant | ~40 000 appels/jour → **≈ 12 jours** |
| Si un quota de 10 000 appels/jour s'applique (annoncé pour actes/comptes) | ≈ 7–8 semaines |
| Ensuite | ~1 650 appels/jour ; les 3 autres exécutions du jour se terminent sans appel |

Les exécutions s'enchaînent sans se chevaucher : budget souple de 5,5 h, timeout de 6 h. En cas de chevauchement accidentel, le checkpoint en compare-and-swap fait échouer la seconde sans rien écraser. Si l'INPI renvoie des 429, l'exécution s'arrête proprement en `partial` avec la raison « quota » dans le log. Le rattrapage ralentit alors tout seul, et pour accélérer il faudrait demander une hausse de quota à l'INPI. Côté GCP, le coût reste de quelques dollars au total (calcul ~0,07 $/h, environ 2 écritures GCS par page, ~200 Go de JSON brut estimés au bout du rattrapage). Voir [les mesures de coûts](couts-rne-dev.md).

## Suivi : logs structurés Cloud Logging

Chaque ligne écrite sur stdout est un objet JSON (`log.py`, sans dépendance). Cloud Run Jobs l'envoie dans Cloud Logging sous forme de `jsonPayload`. Les champs `severity`, `message`, `logging.googleapis.com/labels` (`app=rne-ingest`, `command=sync|bootstrap|materialize`) et `logging.googleapis.com/sourceLocation` sont reconnus. Les logs **ne contiennent jamais** de secrets, de token, de payload d'entreprise ni de SIREN : seulement des compteurs techniques.

| `jsonPayload.event` | Sévérité | Contenu |
|---|---|---|
| `command_started` | INFO | bucket, prefix |
| `sync_started` | INFO | start, completed_until, target, resumes_window |
| `window_completed` | INFO | window_from, window_to, records, pages, api_requests |
| `window_restarted` | WARNING | count ≠ fiches paginées : fenêtre recollectée au prochain run |
| `api_retry` | WARNING | path, status (429, 5xx ou erreur réseau), attempt, delay_seconds |
| `api_token_renewed` | INFO | JWT expiré, renouvelé une fois |
| `command_completed` | INFO / WARNING si `partial` | résumé : status, reason, completed_until, **days_behind**, records, api_requests |
| `command_paused` / `command_failed` | WARNING / ERROR | reason, ou error_type seul pour une exception inconnue |
| `stock_transfer_*`, `stock_member_materialized` | INFO | progression du bootstrap et de la conversion |

Requêtes Logs Explorer (projet `industrial-intelligence-dev`) :

```text
# Résumé de chaque exécution : avancement du rattrapage (days_behind)
resource.type="cloud_run_job" resource.labels.job_name="rne-refresh"
jsonPayload.event="command_completed"

# Tout ce qui mérite attention
resource.type="cloud_run_job" labels.app="rne-ingest" severity>=WARNING

# Quota / erreurs INPI
resource.type="cloud_run_job" jsonPayload.event="api_retry"
```

En CLI : `gcloud logging read 'resource.type="cloud_run_job" AND jsonPayload.event="command_completed"' --freshness=2d --format='value(timestamp,jsonPayload.message)'`.

**Non créé (proposition) :** une alerte Cloud Monitoring sur `severity>=ERROR` pour `rne-refresh`, envoyée par email. Elle demande un canal de notification et une ressource OpenTofu, à valider séparément.

## Commandes

```sh
uv sync --locked
uv run rne-ingest --help

# Essai local dans un namespace jetable, pour ne pas avancer le checkpoint de production
# (ADC + identifiants API dans un .env privé). Supprimer essai/ après usage.
uv run --env-file .env rne-ingest --prefix essai/inpi/rne \
  sync --start 2026-03-01T00:00:00Z --until 2026-03-01T06:00:00Z --max-requests 50

uv run pytest -q && uv run ruff check . && uv run ruff format --check .
```

Variables : `RNE_BUCKET`, `RNE_PREFIX`, `INPI_USERNAME`/`INPI_PASSWORD` (API, compte personnel), `INPI_FTP_USERNAME`/`INPI_FTP_PASSWORD` (FTP, compte technique distinct), `RNE_STOCK_FILE`. Voir `.env.example`. Aucune clé de service account : GCS passe par ADC.

## Mise en service (effectuée le 28/10/2026, procédure conservée)

Le nouveau `sync` écrit sous de **nouveaux chemins** (`control/…/diff/`, `raw|bronze/…/companies/diff/`). Les anciens checkpoints, pages et le catalogue Silver restent intacts : il n'y a rien à migrer ni à effacer.

1. Construire et publier l'image (`docker build --platform linux/amd64 -f Dockerfile.rne …`), puis mettre son digest dans `envs/rne-dev.tfvars` (`rne_image`, `rne_materialize_image`).
2. `tofu plan -var-file=envs/dev.tfvars -var-file=envs/rne-dev.tfvars` puis relire le plan. On y attend : la mise à jour des images et des arguments de `rne-refresh` (`sync --start 2026-03-01T00:00:00Z --max-requests 10000 --max-seconds 20000`), le cron du Scheduler passé à `0 */6 * * *`, la suppression des 4 droits `silver/` devenus inutiles et la levée de la précondition `sync.json`.
3. Appliquer, puis lancer une exécution manuelle et suivre ses logs (`window_completed`, puis le JSON final avec `completed_until` et `days_behind`).
4. Le Scheduler (toutes les 6 h) poursuit ensuite seul le rattrapage, puis le quotidien. Surveiller les premiers jours : `days_behind` doit baisser d'environ 20 par jour. Si `reason` mentionne « quota », le débit est limité par l'INPI.

Les données collectées par l'ancien job (rejeu 2023 et récent du 22 au 27/10) restent disponibles, mais le nouveau flux ne les utilise pas. Le récent sera recollecté quand le rattrapage atteindra ces dates (quelques milliers d'appels).

## Limites connues

- Borne du stock supposée (voir plus haut) ; `stock_cutoff` non certifié.
- Le count INPI est documenté comme un nombre de « liasses ». L'égalité count = fiches paginées a toujours été observée jusqu'ici. Si elle cessait d'être vraie, le job échouerait (de façon visible) au lieu de perdre des données en silence.
- Pas de PDF, bilans, OCR ni bénéficiaires effectifs. Respecter diffusion INSEE/commerciale et retraits.
