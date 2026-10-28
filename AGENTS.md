# AGENTS.md — Industrial Intelligence

Instructions pour les agents de code (Claude Code, Codex, Cursor…) sur tout le dépôt. Le détail vit dans `README.md` et `docs/` : ce fichier ne garde que ce qui ne se devine pas en lisant le code.

## Projet

Projet académique Big Data, à trois développeurs : analyser le tissu économique et industriel français à partir de données ouvertes (SIRENE, INPI RNE, puis BODACC/BOAMP/DECP).

Chaîne visée : **sources → connecteurs Python → GCS raw/bronze (originaux immuables) → ETL silver/gold → API/visualisation** (graphe d'entreprises à terme).

- Cloud-first sur GCP, sans infrastructure locale obligatoire : pas de Docker Compose, base locale ni émulateur. Docker sert uniquement à construire les images Cloud Run.
- Python 3.13 + `uv`, OpenTofu pour l'infra, un seul monorepo. Des connecteurs modulaires et des jobs batch, pas un microservice par source.
- Pas de Kafka, Airflow, Spark, Iceberg, BigQuery ni base graphe sans besoin mesuré et validé par l'équipe. Pour des petits volumes, Polars, DuckDB ou SQL suffisent.

## Principes d'ingénierie

Petite équipe, règle n° 1 : **rester simple**. On traite les cas qui comptent, pas tous les cas imaginables. Un code simple est plus facile à relire, à sécuriser et à modifier.

- **Faire ce qui est demandé, rien de plus.** Pas de fonctionnalité, de refactor ni d'« amélioration » hors de la tâche. Une correction de bug n'implique pas de nettoyer le code autour. Si quelque chose mérite d'être corrigé ailleurs, le signaler sans le faire.
- **Le minimum de code qui résout le problème.** Pas d'abstraction, de helper ni d'option de config pour un seul usage, pas de conception pour des besoins hypothétiques. Trois lignes similaires valent mieux qu'une abstraction prématurée. Si un ingénieur senior trouverait ça trop compliqué, simplifier.
- **Valider aux frontières, faire confiance à l'intérieur.** Valider les entrées externes : APIs INPI et data.gouv, FTP, fichiers sources, arguments CLI. Pas de fallback ni de gestion d'erreur pour des situations impossibles dans notre propre code : échouer bruyamment plutôt que masquer un bug.
- **Réutiliser avant d'écrire.** Chercher un module ou un helper existant et suivre les patterns voisins. Pas de nouvelle dépendance sans raison écrite dans le message de commit.
- **Des diffs chirurgicaux.** Chaque ligne modifiée se rattache à la tâche. Ne pas reformater, renommer ni recommenter du code qu'on n'avait pas besoin de toucher. Supprimer le code mort qu'on a créé ; laisser le code mort préexistant sauf demande.
- **Les commentaires expliquent le _pourquoi_, pas le _quoi_.** Seulement là où la logique ne va pas de soi.

## Façon de travailler

- **Lire avant d'affirmer.** Ne jamais spéculer sur du code, une ressource GCP ou un fichier GCS qu'on n'a pas consulté. L'infra et les données se vérifient en lecture (`gcloud … describe/list`, `gcloud storage ls/cat`, logs).
- **Spécification floue ou vrai choix de conception → s'arrêter et demander.** Un mauvais choix implémenté coûte plus cher qu'une question. Sinon, énoncer son hypothèse et avancer.
- **Planifier avant un changement sur plusieurs fichiers.** Si le diff tient en une phrase, le faire directement.
- **Prouver que ça marche.** Lancer les tests et le lint concernés avant de dire « fini », et montrer le résultat. Ne jamais déclarer réussi un test qui n'a pas tourné.
- **Corriger la cause.** Ne jamais faire taire une erreur, sauter un test, ajouter `# noqa` ou coder une valeur en dur pour faire passer une vérification. Si un test semble faux, le dire au lieu de le contourner.
- **En fin de tâche**, résumer : les fichiers modifiés, les commandes réellement exécutées et leur résultat, et ce qui est déployé ou seulement proposé. Mettre à jour `README.md`/`docs/` si une commande, un contrat ou l'architecture change.

## Commandes

```sh
uv sync --locked                          # dépendances (uv add / uv remove ; commiter uv.lock)
uv run pytest -q                          # tous les tests ; le réseau est bloqué par tests/conftest.py
uv run pytest tests/test_rne_diff.py -q   # un fichier ; -k <nom> pour un test
uv run ruff check . && uv run ruff format --check .
uv run rne-ingest --help                  # CLI RNE : bootstrap, materialize, sync
uv run --env-file .env rne-ingest …       # .env privé, chargé uniquement de façon explicite
```

Ne proposer que des commandes déclarées dans `pyproject.toml` (`[project.scripts]`). Les tests n'utilisent ni réseau ni GCP : les appels sont mockés (`httpx.MockTransport`, `tests/rne_fakes.py:MemoryStore`), avec des fixtures petites et synthétiques.

## Git

- Branche `<type>/<description-courte>` par fonctionnalité ou connecteur (ex. `feat/insee-parquet-parser`).
- **Conventional Commits**, description en français, en minuscules, à l'infinitif, sans point final : `feat(infra): ajouter le bucket gcs de dev`. Changement incompatible : `!` ou pied `BREAKING CHANGE:`.
- Indexer des chemins précis (`git add <chemins>`), puis vérifier `git diff --cached` et `git diff --cached --check`. Jamais de secrets, de `.env`, de `*.tfstate`/`*.tfplan` ni d'artefacts générés.
- Les dossiers vides gardent un `.gitkeep`.

## GCP, OpenTofu et coûts

Projet `industrial-intelligence-dev`. Réellement déployé (vérifié le 28/10/2026) :

- bucket `gs://industrial-intelligence-dev-data` : `EU` multi-région, privé, `force_destroy = false` ;
- Artifact Registry `rne` (`europe-west9`) et 4 secrets INPI dans Secret Manager ;
- Cloud Run Jobs `rne-bootstrap`, `rne-materialize`, `rne-refresh` (`sync`), et Scheduler `rne-refresh-daily` toutes les 6 h ;
- données RNE sous le préfixe `inpi/rne` (`raw/`, `bronze/`, `control/`). Rien d'autre (BigQuery, Workflows…) n'est créé.

Règles :

- **Demander un accord explicite avant** : `tofu apply`/`destroy`, tout changement IAM ou de facturation, un déploiement d'image sur un job, un téléchargement massif, de l'OCR payant, ou toute suppression ou écrasement de données GCS. Annoncer le projet, la ressource, le volume et le coût estimé.
- Toujours planifier avec les deux fichiers : `tofu plan -var-file=envs/dev.tfvars -var-file=envs/rne-dev.tfvars -out=<nom>.tfplan`. Sans `rne-dev.tfvars`, le plan propose de détruire les ressources RNE.
- L'état OpenTofu est **local, sur le poste de Léo** : un seul opérateur à la fois, et sauvegarder `terraform.tfstate` avant chaque apply.
- Ne pas changer la région, le nom, l'accès public ni `force_destroy` du bucket : sa localisation ne se modifie pas sans le recréer.
- Images Cloud Run : `docker build --platform linux/amd64 -f Dockerfile.rne`, épinglées **par digest** dans les tfvars. Procédure complète : `docs/rne/inpi-ingestion.md`.
- Essais : petits échantillons, limites explicites. Ne jamais écraser des données existantes.
- Authentification par ADC uniquement (`gcloud auth application-default login`) : jamais de clé JSON de service account. Les identifiants `gcloud` CLI et ADC peuvent différer.

## Données : règles métier

- `siren` est l'identifiant canonique d'entreprise : une **chaîne de 9 caractères** dont on garde les zéros initiaux. `siret` (établissement) fait 14 caractères. Nom et adresse ne sont pas des identifiants.
- **Dirigeant ≠ actionnaire**, **établissement ≠ filiale**, **co-localisation ≠ lien de propriété**. Une relation de propriété exige un document explicite, avec date, source et niveau de confiance. Distinguer fait extrait, inférence et inconnu.
- Le registre des bénéficiaires effectifs est restreint : ne jamais le présumer en open data.
- Les originaux `raw/` sont immuables. Toujours conserver la provenance : source, URL ou identifiant distant, `retrieved_at` UTC, sha256, taille, version.
- Horodatages techniques en UTC ; les dates juridiques et métier restent telles que fournies.
- Gros volumes : traitement en colonnes (Arrow, Parquet) et en streaming. Pydantic seulement pour les configs et métadonnées, pas ligne par ligne.

### Pièges INPI RNE (constatés)

- Le stock FTP (`stock_RNE_formalites_…20260304…zip`) date d'environ mars 2026. Le différentiel repart de `2026-03-01T00:00:00Z`, une **hypothèse non certifiée** par l'INPI.
- `/companies/diff` renvoie la **fiche actuelle** des entreprises modifiées dans la fenêtre, pas leur état passé. Ne pas rejouer le diff depuis 2023 : les événements historiques sont déjà dans `historique[]` du stock.
- `/companies/diff/count` plafonne à 10 000 ; au-delà, couper la fenêtre. Quota annoncé de 10 000 appels/jour, non vérifié pour le diff. Un 429 doit mettre le job en pause, pas le faire échouer.
- Le JWT de login dure 24 h. Ne jamais l'afficher, le logger ni le commiter.
- Les identifiants FTP (stock, `www.inpi.net`) et SFTP/API (compte personnel) sont **distincts** : ne jamais les intervertir.
- Les `bilansSaisis` sont déjà en JSON : pas d'OCR dessus. Un HTTP 500 sur l'historique ne prouve pas une absence de droits : tracer l'erreur, réessayer raisonnablement, continuer.
- Ne pas inventer d'endpoint ni de paramètre : se référer aux docs techniques INPI citées dans `docs/rne/inpi.md`.

## Connecteurs et logs

- Un connecteur : découvre les ressources, télécharge en flux, écrit le brut et sa provenance dans GCS, gère l'auth, la pagination, le backoff (429/5xx, `Retry-After`) et la reprise.
- **Idempotent**, et le **checkpoint n'avance qu'après succès**. Pas de boucle infinie : budgets d'appels et de temps explicites.
- Logs : `log.py:log_event(logger, "nom_evenement", **champs)`, qui produit du JSON structuré lu par Cloud Logging. **Jamais** de secret, de token, de payload d'entreprise ni de SIREN dans les logs : seulement des compteurs techniques.
- Une PR de nouveau connecteur documente : la source et son authentification, une exécution reproductible, les tests, les limites, la fréquence de mise à jour et le schéma de sortie.

