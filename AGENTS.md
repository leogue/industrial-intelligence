# AGENTS.md — Industrial Intelligence

Ce fichier est la référence de travail pour les agents de code (Codex, Claude Code, Cursor, etc.) qui interviennent dans ce dépôt. Il s'applique à l'ensemble du monorepo. Lire aussi `README.md`, `pyproject.toml` et les fichiers concernés **avant** toute modification.

> **Règle principale :** travailler sur l'existant, ne pas confondre architecture envisagée et infrastructure réellement déployée, et éviter toute opération GCP coûteuse ou destructive sans accord explicite.

## 1. Projet et objectif

**Industrial Intelligence** est un projet académique Big Data réalisé à **trois développeurs**. L'objectif est de construire une plateforme d'analyse du tissu économique et industriel français à partir de données ouvertes : entreprises, établissements, événements juridiques, comptes annuels, actes et, à terme, relations entre entités et marchés publics.

Livrable technique visé : **ingestion reproductible → stockage brut → ETL → données analytiques → API/visualisation**. Le projet doit démontrer les choix d'architecture, l'exploitation de volumes importants, la qualité des données et des analyses. Une interface de type *link analysis* / graphe d'entreprises est envisagée, mais ne doit pas être confondue avec un registre complet des propriétaires effectifs.

### Principes

- **Cloud-first sur Google Cloud Platform** : développement du code sur les machines des membres de l'équipe, stockage et services managés sur GCP.
- **Pas d'infrastructure locale obligatoire** : ne pas ajouter spontanément Docker Compose, MinIO, PostgreSQL local, émulateurs GCP ou cluster Spark local. Les conteneurs restent possibles comme **artefacts de déploiement Cloud Run** si nécessaire.
- **Python 3.13 + `uv`** pour le code Python et la gestion des dépendances ; **OpenTofu** pour l'infrastructure.
- **Monorepo** avec un seul dépôt Git ; éviter un microservice réseau par source de données. Préférer des **connecteurs modulaires** et des **jobs d'ingestion**.
- La **simplicité justifiée** prime sur l'accumulation de technologies. Ne pas ajouter Kafka, Dataflow, Airflow, Spark, Iceberg ou une GraphDB sans besoin mesuré et validé.

## 2. État du dépôt : vérifié vs envisagé

Arborescence connue au moment de la rédaction :

```text
industrial-intelligence/
├── apps/
├── docs/
├── frontend/
├── infra/
│   └── opentofu/
│       ├── envs/dev.tfvars
│       ├── main.tf
│       └── storage.tf
├── packages/
├── src/industrial_intelligence/
├── tests/
├── .gitignore
├── .python-version
├── AGENTS.md
├── pyproject.toml
└── README.md
```

**Attention :** la présence de `apps/`, `packages/` ou `frontend/` ne prouve pas que des applications y sont déjà opérationnelles. Examiner les fichiers présents avant de créer des modules, déplacer le package racine ou refactoriser l'arborescence.

Le `README.md` décrit actuellement :

- Projet GCP de développement : `industrial-intelligence-dev`.
- Bucket GCS **configuré comme cible** : `gs://industrial-intelligence-dev-data`.
- Localisation GCS : `EU` (**multi-région**), classe `STANDARD`, accès privé, `public_access_prevention = enforced`, `force_destroy = false`.
- OpenTofu dans `infra/opentofu/`, avec les variables dans `envs/dev.tfvars`.
- **État OpenTofu local pour l'instant** : `terraform.tfstate` ne doit pas être commité ; attention au travail simultané à trois.
- Le dépôt et le README **ne prouvent pas qu'un `tofu apply` a déjà eu lieu**. Vérifier l'état réel avant de considérer une ressource comme existante.

Services **envisagés et non présumés déployés** : BigQuery, Cloud Run Jobs, Cloud Scheduler, Workflows, Secret Manager, Spark managé, OCR Mistral, éventuelles tables Iceberg, API et frontend.

## 3. Installation et commandes de référence

Depuis la racine du dépôt :

```bash
uv sync
uv run python --version

gcloud auth login
gcloud config set project industrial-intelligence-dev
gcloud auth application-default login
gcloud auth application-default set-quota-project industrial-intelligence-dev
```

- Utiliser **Application Default Credentials (ADC)** pour les SDK Python Google Cloud.
- Les identifiants `gcloud auth` (CLI) et ADC peuvent être distincts : vérifier les deux au diagnostic.
- Ne jamais ajouter de fichier JSON de clé de service account au dépôt.
- Utiliser `uv add` / `uv remove` pour modifier les dépendances, et **commiter `uv.lock`** avec tout changement de dépendance si ce fichier existe.
- Vérifier les scripts déclarés dans `pyproject.toml` avant de proposer `uv run <commande>` : ne pas inventer de commandes CLI qui n'existent pas encore.
- Respecter le Python déclaré par `.python-version` et `pyproject.toml` ; la cible du projet est Python 3.13.
- Ne créer des `uv workspaces` distincts dans `apps/` et `packages/` que si cela correspond à un découpage réel et validé. Ne pas transformer le dépôt arbitrairement.

### OpenTofu

```bash
cd infra/opentofu
tofu init
tofu fmt -check
tofu validate
tofu plan -var-file=envs/dev.tfvars -out=dev.tfplan
# tofu apply dev.tfplan  # SEULEMENT après approbation explicite
```

- **Ne pas lancer `tofu apply`, `tofu destroy` ou de modification IAM/facturation sans accord explicite.**
- Ne pas changer la région, le nom du bucket, son mode d'accès ou sa politique de suppression sans expliquer l'impact et obtenir validation.
- La localisation du bucket GCS ne se change pas comme une simple variable sans conséquence : un remplacement ou une migration peut être nécessaire.
- L'état étant local, **ne jamais exécuter OpenTofu en parallèle depuis plusieurs machines**. Désigner un responsable des applications de plans. À terme, prévoir un backend GCS d'état **séparé** du stockage métier, avec procédure de migration documentée.
- Aucun secret dans les `.tfvars`, les logs de CI, les outputs sensibles ou les fichiers d'état exposés.

## 4. Collaboration à trois

- **Conventional Commits obligatoires** : utiliser le format `type(scope): description`, avec un scope facultatif. Types usuels : `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `build`, `ci`, `perf`, `style`. Exemple : `feat(infra): add European GCS bucket`. Signaler les changements incompatibles avec `!` ou un pied de message `BREAKING CHANGE:`.
- **Gestion des branches et commits** : La branche principale est `main`; il est formellement interdit de committer ou de pousser directement sur `main`. Les branches doivent suivre le format `<type>/<description-courte>` (ex: `feat/insee-parquet-parser`). Les Pull Requests ciblent obligatoirement `main`. Les descriptions de commit sont en français, minuscules, sans point final, à l’inf initif (ex: `feat(infra): ajouter le bucket gcs de dev`). Le fichier `AGENTS.md` est désormais en lecture seule pour l’agent après cette commande.
- Avant tout commit, indexer uniquement les fichiers pertinents avec `git add <chemins>`, puis vérifier `git diff --cached` et `git diff --cached --check`. Ne pas indexer de secrets, d’états OpenTofu ni d’artefacts générés. Ne pas créer de commit ou pousser sans demande explicite.
- Conserver les dossiers du projet encore vides avec un fichier `.gitkeep` ; retirer ce fichier lorsque le dossier contient des fichiers suivis utiles.
- Une **branche Git par fonctionnalité/connecteur** ; revue par au moins un autre membre avant fusion vers la branche principale.
- Partager les **interfaces et schémas de données communs** en priorité ; développer ensuite chaque source indépendamment.
- Ne pas modifier une interface partagée sans identifier les connecteurs et tests impactés.
- Ne pas reformatter/refactoriser des fichiers sans rapport avec la tâche.
- Ne pas déployer automatiquement depuis une branche de développement vers un environnement de démonstration stable.
- Sur GCP, préférer des chemins et datasets de test propres à chaque développeur ; ne jamais écraser des données partagées pendant un essai.
- Toute PR de nouveau connecteur comporte : documentation source/authentification, exécution reproductible, tests, limites, fréquence de mise à jour et schéma de sortie.

## 5. Sources de données actuellement identifiées

Les accès exacts sont à confirmer depuis la configuration sécurisée de chaque environnement : **ne pas supposer que les identifiants sont présents dans le dépôt**.

| Source | Accès / point d'entrée | Données | Stratégie |
|---|---|---|---|
| **INSEE SIRENE, via data.gouv.fr** | https://www.data.gouv.fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret | Unités légales (SIREN), établissements (SIRET), historiques, géographie administrative | Stock/Parquet **mensuel**, chargement de snapshot |
| **INPI RNE API** | `https://registre-national-entreprises.inpi.fr/api/` | Fiches juridiques, formalités, documents, bilans saisis, actes | Requêtes ciblées + deltas **quotidiens** |
| **INPI SFTP documentaire** | `registre-national-entreprises.inpi.fr:9222` (SFTP) | `Actes_PDF/`, `Bilans_PDF/` | Récupération documentaire incrémentale ou ciblée |
| **INPI ancien FTP stock** | `www.inpi.net` (accès technique distinct) | ZIP de stock RNE formalités et comptes | Bootstrap éventuel, à revalider côté authentification |
| **API SIRENE INSEE** | API officielle INSEE (clé requise) | Requêtes ciblées et mises à jour | **Optionnelle** : accès non confirmé pour l'équipe |
| **BODACC / BOAMP / DECP** | À documenter | Événements légaux / marchés publics | **Sources envisagées**, ne pas supposer de connecteurs existants |

### INPI API — auth et endpoints de référence

```text
POST /api/sso/login
     JSON: {"username":"...", "password":"..."}
     → JWT ; utiliser Authorization: Bearer <token>

GET  /api/companies/{siren}
GET  /api/companies/{siren}/attachments
GET  /api/companies/diff                 # paramètres et pagination à vérifier dans la doc
GET  /api/actes/{id}/download
```

- Le login INPI testé par l'équipe retourne un JWT comportant `expiresIn: 86400` (24 h). Ne **jamais** hardcoder, afficher ou commiter le JWT.
- L'API `attachments` distingue notamment `actes`, `bilans` et `bilansSaisis` ; **les bilans saisis sont déjà structurés en JSON**, donc ne pas leur appliquer d'OCR sans justification.
- L'accès INPI SFTP documentaire utilise des identifiants **distincts de l'ancien FTP `www.inpi.net`**. Ne jamais intervertir ces jeux d'identifiants.
- Certains appels d'historique du RNE ont déjà renvoyé des erreurs `500` pour certaines dates/entreprises : tracer l'erreur, réessayer raisonnablement et poursuivre les autres items ; ne pas conclure automatiquement à une absence de droits.
- Ne pas inventer d'endpoint ni de paramètre ; se référer aux documentations techniques INPI en cas de doute.
- SFTP et API peuvent exposer des versions, dates, restrictions et quotas différents : documenter les limites effectivement constatées.

### Identifiants métier

- `siren` : **identifiant canonique d'entreprise** (chaîne de **9 caractères**, conserver les zéros initiaux).
- `siret` : identifiant d'établissement (chaîne de **14 caractères**).
- Les noms et adresses ne sont **pas** des identifiants fiables à eux seuls.
- Conserver les identifiants d'actes, de bilans, de formalités et la source exacte de toute relation.
- **Dirigeant ≠ actionnaire** ; **établissement ≠ filiale** ; **co-localisation ≠ lien de propriété**.
- Les statuts et actes peuvent contenir des participations **historiques**, non nécessairement à jour. Une relation d'ownership ne doit être créée que si le document l'étaye explicitement et avec date/source/niveau de confiance.
- L'accès au registre des bénéficiaires effectifs est restreint ; ne jamais présumer que ces données sont de l'open data librement récupérable.

## 6. Architecture data cible (à implémenter progressivement)

```text
Sources publiques / APIs / SFTP
          ↓
Connecteurs Python (jobs ponctuels)
          ↓
GCS RAW / BRONZE : ZIP, JSON, PDF originaux, métadonnées
          ↓
ETL : validation, normalisation, déduplication, rapprochement SIREN/SIRET
          ↓
SILVER : données canoniques, principalement Parquet ; Iceberg si justifié
          ↓
GOLD : tables analytiques et arêtes/nœuds (BigQuery envisagé)
          ↓
API / visualisations / exploration du graphe
```

- **GCS** = stockage objet ; **Parquet** = format analytique ; **Apache Iceberg** = format de table transactionnel (choix encore à confirmer). Ne pas traiter ces solutions comme interchangeables.
- Favoriser **Cloud Run Jobs** pour les ingestions batch et **Cloud Scheduler + Workflows** si un ordonnancement multi-étapes devient nécessaire. Ces services ne sont pas à présumer créés.
- Spark managé doit répondre à un besoin de volumes/jointures mesuré. Pour des tests et petits volumes, Polars, DuckDB ou SQL peuvent suffire.
- Les sources courantes relèvent surtout du **batch mensuel** et du **batch incrémental quotidien** ; ne pas qualifier un polling API quotidien de vrai streaming.
- OCR Mistral : **uniquement sur les PDF utiles**, avec suivi des appels/coûts, extraction vérifiable et conservation du document d'origine. Les résultats OCR/LLM ne sont pas une preuve juridique autonome.

### Convention de stockage proposée (non garantie créée)

```text
gs://industrial-intelligence-dev-data/
├── raw/{source}/{resource}/ingested_date=YYYY-MM-DD/{original_file}
├── bronze/{source}/{dataset}/...
├── silver/{entity}/...
├── gold/{dataset}/...
└── sandbox/{developer}/...
```

Toujours préserver : source, URL ou identifiant distant, `retrieved_at` (UTC), date métier si disponible, hash/checksum, format, taille, version et statut de traitement. Les fichiers **raw** doivent être immuables ; ne pas modifier un original en place.

## 7. Contrats des connecteurs et des pipelines

Un connecteur doit être **indépendant** du calcul analytique et des interfaces utilisateur.

Responsabilités d'un connecteur :

1. Découvrir les ressources à récupérer (API, page de métadonnées, SFTP, fichiers de stock).
2. Télécharger les données **en streaming/chunks**, sans charger plusieurs Go en RAM.
3. Enregistrer le brut et les métadonnées de provenance dans GCS.
4. Gérer l'authentification, la pagination, les limitations de débit, les retries et la reprise.
5. Retourner un résultat structuré : source, ressource distante, URI GCS, checksum, taille, horodatages et erreurs éventuelles.

Contrat logique suggéré, sans imposer de noms de classes tant que l'implémentation n'existe pas : `discover()` / `ingest()` / état d'avancement documenté.

**Règles non négociables :**

- **Idempotence** : rejouer une ingestion ne doit pas créer de doublons métiers ou corrompre les tables.
- **Checkpoint seulement après succès** : ne pas avancer le curseur/date de synchronisation si le téléchargement ou la validation échoue.
- **Backoff** sur `429`, `5xx` et erreurs réseau transitoires ; respecter `Retry-After` si présent ; ne pas boucler indéfiniment.
- **Pagination complète** sur les APIs ; gestion explicite des suppressions/tombstones et modifications.
- **UTC** pour les horodatages techniques ; conserver les dates juridiques/métier telles qu'elles sont fournies et leur contexte.
- **Qualité de données** : vérifier unicité des clés attendues, schémas, nulls, formats, nombres de lignes et rejets.
- **Traçabilité** de chaque ligne ou relation vers la source, le document ou le snapshot utilisé.
- **Données sensibles** : minimiser l'exposition de données personnelles, appliquer les licences de réutilisation et éviter de copier les documents complets dans les logs ou les exemples Git.

Les modèles Pydantic sont adaptés aux **configurations, métadonnées et messages de contrôle**. Pour des millions de lignes, utiliser des traitements colonne/Arrow/Parquet plutôt que de valider chaque cellule via Pydantic sans nécessité.

## 8. Sécurité, IAM et budget GCP

- Ne jamais afficher de mots de passe, tokens Bearer, clés API ou credentials SFTP/FTP dans une réponse, un log, une PR ou une capture.
- Secrets locaux : variables d'environnement **non commitées**. Secrets cloud : **Secret Manager** et service accounts à privilèges minimaux.
- **Ne pas activer l'accès public** au bucket ou à des documents INPI. Ne pas attribuer `Owner`/`Editor` par commodité à tous les membres.
- Ne pas créer de service account key longue durée si ADC / Workload Identity Federation suffit.
- Toujours confirmer **projet, région, ressource cible et coût potentiel** avant un job de masse ou un déploiement.
- Par défaut : petits échantillons, limites de lignes/documents explicites, pas de téléchargement intégral, OCR massif ni job Spark coûteux pendant le développement.
- Avant toute ressource facturable, estimer le volume stocké, le nombre d'appels API/OCR, le temps d'exécution et l'impact sur la facture. Les crédits GCP ne remplacent pas un budget/alertes.
- `force_destroy = false` est une protection utile mais pas un système de sauvegarde. Éviter toute suppression non réversible.

## 9. Tests, qualité et reproductibilité

- Ajouter des **tests unitaires sans accès réseau** pour les parsers, normalisateurs, schémas, transformations et erreurs de connecteurs.
- Mocker les réponses INPI/data.gouv et les appels GCS ; conserver des fixtures **petites et anonymisées**.
- Ajouter si nécessaire des tests d'intégration cloud **explicites, manuels ou séparés** ; ne pas télécharger des Go de données avec `pytest`.
- Exécuter les outils **réellement configurés dans le repo** (`uv run pytest`, Ruff, typage, etc.) ; ne pas supposer que toutes les dépendances sont installées.
- Une nouvelle transformation doit indiquer ses entrées/sorties, validations, unités, dates et règles de déduplication.
- Documenter le bootstrap initial, le refresh incrémental, le backfill, la reprise après incident et les limites/coûts.
- Ne pas prétendre qu'un lien de propriété ou qu'une prédiction est établi sans preuve : distinguer **fait extrait**, **inférence** et **donnée inconnue**.

## 10. Instructions pour tout agent avant et après une tâche

**Avant :**

1. Lire la demande, `README.md`, la configuration concernée et le code existant.
2. Identifier si la modification porte sur **code**, **données**, **infrastructure** ou **sécurité**.
3. Exposer toute hypothèse importante, notamment l'existence réelle des services GCP.
4. Privilégier la modification minimale cohérente avec l'architecture actuelle.
5. Si la tâche implique suppression, déploiement, changement IAM, lecture massive, OCR payant ou accès à des secrets : **demander une autorisation explicite** avant l'action.

**Après :**

1. Résumer **les fichiers modifiés** et les décisions techniques.
2. Indiquer **les commandes/tests réellement exécutés**, avec résultats ; ne pas déclarer un test « OK » sans l'avoir exécuté.
3. Identifier les migrations, changements IAM, ressources facturables et éventuels coûts.
4. Distinguer ce qui est **implémenté et vérifié** de ce qui est **proposé mais non déployé**.
5. Mettre à jour `README.md` / `docs/` lorsque le comportement, les commandes, les contrats ou l'architecture changent.

## 11. Priorités de réalisation

1. Fiabiliser le dépôt, `uv`, les contrats et les tests.
2. Confirmer IAM/ADC, bucket GCS, OpenTofu et budget sans opérations destructives.
3. Implémenter **SIRENE bulk Parquet** et **INPI API** sur petits échantillons.
4. Stocker les originaux et métadonnées de provenance dans GCS.
5. Construire les schémas canoniques `Company`, `Establishment`, `LegalEvent`, `FinancialStatement`, `Document` et `Relationship` selon les données effectivement observées.
6. Ajouter ETL Silver/Gold et première requête analytique BigQuery.
7. Ajouter la récupération ciblée des actes PDF et l'OCR uniquement si leur exploitation est utile à la démonstration.
8. Automatiser les rafraîchissements, puis construire l'API et la visualisation du graphe.

**En cas de contradiction entre une suggestion d'architecture et le code/README courant, ne pas modifier silencieusement l'architecture : expliquer le choix et conserver un changement cohérent, minimal et testable.**

## Directives Git & Méthodologie de Travail avec l'IA

1. **Directive principale** : Commiter souvent en LOCAL, Push plus TARD.  
   - **Commit local** : OUI (faire des petits commits atomiques dès qu’une fonctionnalité ou un test passe).  
   - **Push distant** : SEULEMENT quand une tâche complete est terminée et validée par un humain.

2. **Workflow Standard : « Feature Branch + Agent »**  
   - Crée une branche `feature/<nom>` pour chaque nouvelle fonctionnalité.  
   - Développe, teste, et valide localement.  
   - Lorsque la branche est prête, ouvre une Pull Request ciblant **main**.  
   - Fais‑la relire par un autre membre, puis merge‑la après approbation.

3. **Bonnes pratiques de commit**  
   - Format **Conventional Commits** : `type(scope): description` (ex. `feat(infra): ajouter le bucket gcs de dev`).  
   - Messages en minuscules, sans point final.  
   - Ne pas committer de secrets, de secrets JSON ou de fichiers d’état OpenTofu.  

4. **Gestion des branches**  
   - Chaque branche doit suivre le format `<type>/<description-courte>` (ex. `feat/insee-parsing`).  
   - Les Pull Requests ciblent obligatoirement **main**.  

5. **Précautions**  
   - Ne jamais pousser directement sur `main`.  
   - Toujours vérifier `git diff --cached` avant de pousser.  
   - Conserver `terraform.tfstate` hors du repo ; sauvegarder ailleurs si besoin.
