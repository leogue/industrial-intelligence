# Déploiement RNE de développement — journal du 22 octobre 2026

Ce journal décrit les actions **réellement effectuées**, à distinguer du [guide de déploiement](inpi-ingestion.md) et des plans préparés mais non appliqués.

## Cible et protections

- Projet : `industrial-intelligence-dev`.
- Région du registre et des jobs : `europe-west9` (Paris).
- Bucket existant : `gs://industrial-intelligence-dev-data`, localisation `EU`, accès uniforme et prévention d'accès public `enforced`, vérifiés via ADC.
- Namespace de test configuré : `sandbox/leo/inpi/rne`, soit `raw/sandbox/leo/inpi/rne/` et `control/sandbox/leo/inpi/rne/`.
- État OpenTofu local : un seul opérateur à la fois ; copie de sauvegarde privée réalisée avant application. Aucun état, plan ou fichier `.env` à commiter.
- Branche de préparation : `chore/deploy-rne`, issue de `origin/main` après fusion de la PR d'ingestion.

## Phase 1 — appliquée et vérifiée

Le plan `rne-prerequisites.tfplan` approuvé a été appliqué : **7 créations, 0 modification, 0 suppression**.

| Ressource | État constaté |
|---|---|
| API Artifact Registry | Activée via OpenTofu |
| API Secret Manager | Activée via OpenTofu |
| Registre Docker `rne` | Créé à `europe-west9` |
| Secret `inpi-rne-api-username` | Créé ; version 1 ajoutée et activée |
| Secret `inpi-rne-api-password` | Créé ; version 1 ajoutée et activée |
| Secret `inpi-rne-ftp-username` | Créé ; version 1 ajoutée et activée |
| Secret `inpi-rne-ftp-password` | Créé ; version 1 ajoutée et activée |

Les quatre valeurs ont été lues depuis l'environnement chargé explicitement à partir du `.env` privé et transmises à `gcloud secrets versions add` **par stdin**. Elles n'ont pas été affichées, placées dans des arguments CLI, des variables OpenTofu ou un état. Aucun JWT n'a été enregistré. Aucune attribution IAM manuelle aux comptes d'exécution n'a été faite pendant cette phase.

## Image initiale — publiée et vérifiée

Image locale construite/testée : `industrial-intelligence-rne:dev`, architecture **linux/amd64**, depuis le Mac ARM.

Tag publié :

```text
europe-west9-docker.pkg.dev/industrial-intelligence-dev/rne/ingestion:dev-a63befd-amd64
```

Référence immuable vérifiée dans Artifact Registry et utilisée pour le premier déploiement (remplacée ensuite par l'image archive seule décrite plus bas) :

```text
europe-west9-docker.pkg.dev/industrial-intelligence-dev/rne/ingestion@sha256:1ead5f9eb5ceedda4b936cb510595cb08481c89553b0979aadbc00f779f27296
```

Publication effectuée via le credential helper `gcloud`, sans fichier de clé de service account. Le helper local Docker a été configuré pour `europe-west9-docker.pkg.dev`.

## Phase 2 — appliquée, jobs prêts et Scheduler en pause

Les **20 créations approuvées** ont été réalisées :

- activation des API Cloud Run, Cloud Scheduler et IAM ;
- trois service accounts : bootstrap, refresh, Scheduler ;
- quatre accès Secret Manager limités aux secrets nécessaires à chaque job ;
- six autorisations GCS conditionnelles sur les préfixes de test (création/lecture raw, manipulation control) ;
- deux Cloud Run Jobs `rne-bootstrap` et `rne-refresh`, une tâche chacun, 1 vCPU / 1 GiB, même image par digest ;
- droit d'invocation du Scheduler sur le seul job refresh ;
- un Scheduler `rne-refresh-daily`, quotidien à 05:00 UTC, **état réel `PAUSED`**.

Le fichier `infra/opentofu/envs/rne-dev.tfvars` décrit cette configuration. Le bucket et les ressources de la phase 1 n'ont été ni remplacés ni supprimés ; la politique IAM du bucket a reçu les membres conditionnels approuvés.

### Incident de création et reprise sans suppression

La première application a créé les comptes et les autorisations, mais les deux jobs ont répondu `Ready=False`, erreur interne Cloud Run code 7. Le rôle standard `roles/run.serviceAgent` était présent. La cause exacte de l'erreur transitoire n'a pas été établie.

Une mise à jour des deux jobs avec **le même digest**, sans exécution, suppression ni nouveau droit IAM, a réconcilié leur état : `Ready=True`. Les spécifications d'exécution avant/après ont été comparées et étaient identiques. Après sauvegarde de l'état et vérification externe de cette disponibilité, les deux marqueurs `tainted` ont été retirés dans l'état OpenTofu pour éviter une recréation inutile. **Ne pas reproduire un `untaint` sur une ressource dont la santé/conformité n'a pas été vérifiée.**

Le plan de reprise vérifié contenait uniquement les deux créations restantes (invocation et Scheduler) et la remise à zéro des métadonnées `client` / `client_version` introduites par `gcloud` sur les jobs. Il a été appliqué : **2 créations, 2 modifications de métadonnées, 0 suppression**, sans changement de configuration d'exécution.

### Alignement de configuration et contrôle final

Cloud Scheduler omet dans sa réponse un bloc `retry_config` entièrement à zéro. Le bloc `retry_count = 0` du code provoquait donc un faux diff permanent. Il a été remplacé par un commentaire explicitant les valeurs par défaut documentées **`retryCount=0` et `maxRetryDuration=0`**, qui désactivent les retries entre deux échéances. Aucun retry n'a été activé et aucun apply supplémentaire n'a été nécessaire pour cette correction de représentation.

Contrôle final effectué :

```sh
# Depuis infra/opentofu ; vérification seulement.
tofu plan -input=false -detailed-exitcode \
  -var-file=envs/dev.tfvars -var-file=envs/rne-dev.tfvars
```

Résultat initial : **`No changes`**, code de sortie 0. Les deux jobs étaient `Ready`, le Scheduler `PAUSED`. Les commandes étaient `bootstrap --allow-large-download` / 24 h et `refresh` / 6 h ; les overrides des tests de démarrage n'ont pas modifié les jobs. Le mode archive seule a ensuite été activé explicitement ci-dessous.

## Tests de démarrage réellement exécutés

Deux exécutions manuelles ont remplacé les arguments par `--help`, avec **une tâche et un timeout de 60 s** :

| Job | Exécution | Résultat | Fin UTC |
|---|---|---|---|
| `rne-refresh` | `rne-refresh-gstjs` | Completed=True, 1 tâche réussie ; durée d'exécution annoncée 7,42 s | 2026-10-22 14:40:00 |
| `rne-bootstrap` | `rne-bootstrap-zmxz6` | Completed=True, 1 tâche réussie ; durée d'exécution annoncée 7,47 s | 2026-10-22 14:40:12 |

Commande utilisée, pour chaque nom de job :

```sh
gcloud run jobs execute rne-refresh \
  --project=industrial-intelligence-dev --region=europe-west9 \
  --args=--help --tasks=1 --task-timeout=60s --wait
```

Cela vérifie l'import de l'image, le démarrage de l'entry point et la résolution des références Secret Manager. **Cela ne valide ni l'authentification INPI, ni les écritures GCS par les workers, ni une ingestion complète.** Aucun appel métier INPI et aucun téléchargement de stock n'ont été lancés par ces exécutions.

À l'issue de ces tests de démarrage, les absences de `control/sandbox/leo/inpi/rne/checkpoint.json` et de `lock.json` ont été vérifiées via ADC. Aucun checkpoint artificiel n'a été créé. Le fichier de stock et la borne initiale n'étaient pas encore configurés à cette étape.

## Bootstrap complet en mode archive seule — réussi

Après autorisation explicite du transfert complet et d'une enveloppe indicative de 5 €, le mode `--archive-only` a été ajouté et testé : **71 tests hors réseau réussis**, Ruff et verrou de dépendances vérifiés. Ce mode archive le stock sans lire/écrire le checkpoint différentiel et peut réutiliser ensuite le reçu du même stock. Des compteurs techniques de progression ont été ajoutés, sans données personnelles ni secrets.

Une correction du stockage a aussi été testée : `BlobWriter` ne renseigne pas automatiquement `Blob.generation` après fermeture ; le code relit désormais les métadonnées serveur et compare la taille avant d'écrire un reçu. Deux tests ciblés couvrent la génération initialement absente et une taille serveur incohérente.

Nouvelle image AMD64 construite, testée localement puis publiée :

```text
europe-west9-docker.pkg.dev/industrial-intelligence-dev/rne/ingestion:archive-only-20261022
europe-west9-docker.pkg.dev/industrial-intelligence-dev/rne/ingestion@sha256:76ac9c78fdf095922a1261afdb6dfea84702219ef3807125a1fb5e604ecbd35b
```

Le plan vérifié puis appliqué ne contenait que **2 mises à jour en place**, aucun ajout/suppression ni changement IAM : image commune des jobs, activation de `rne_bootstrap_archive_only = true` et nom explicite du stock. La borne `rne_initial_from` est restée vide ; le Scheduler est resté en pause.

### Exécution et résultat réels

| Information | Valeur constatée |
|---|---|
| Exécution | `rne-bootstrap-ffq4s` |
| Début UTC | 2026-10-22 15:07:04 |
| Fin UTC | 2026-10-22 15:18:55 |
| Résultat Cloud Run | **Completed=True**, 1 tâche réussie, 0 échec |
| Durée annoncée | **11 min 50,18 s** |
| Fichier FTP | `stock_RNE_formalites_NIVEAU1_20260304_1400.zip` |
| Taille transférée et persistée | **15 135 083 838 octets** |
| MDTM distant vérifié | `20261019090647` |
| Génération GCS | `1792682330700428` |
| CRC32C retourné par GCS | `tO86ZA==` |
| Statut applicatif | `archived`, `checkpoint_written=false`, `reused_archive=false` |

SHA-256 calculé pendant le transfert et conservé dans les manifestes :

```text
aebbad44755901448b9d5785cca16524dcae20852df60899f43297b205116557
```

Objet brut privé créé :

```text
gs://industrial-intelligence-dev-data/raw/sandbox/leo/inpi/rne/stocks/b47860c7c6a645bc1d54e878d2c4d2b5ec67b2d4530da9b1bd5f2cd2c8c9fbde/5dcffb1a692b4469af50f07204107340/stock_RNE_formalites_NIVEAU1_20260304_1400.zip
```

Le manifeste est au même chemin avec le suffixe `.manifest.json`. Le reçu réutilisable est :

```text
gs://industrial-intelligence-dev-data/control/sandbox/leo/inpi/rne/stocks/b47860c7c6a645bc1d54e878d2c4d2b5ec67b2d4530da9b1bd5f2cd2c8c9fbde.json
```

Vérifications indépendantes après succès, **sans retélécharger le ZIP** : objet GCS présent, taille/génération identiques au reçu, CRC32C disponible, manifeste et reçu JSON identiques. Le SHA-256 du manifeste provient du calcul en flux ; il n'a pas été recalculé par une seconde lecture de 15 Go depuis GCS. Le verrou et `checkpoint.json` sont absents, et le Scheduler est toujours `PAUSED`. Le contrôle final OpenTofu retourne de nouveau **`No changes`**.

Ce test valide désormais l'authentification FTP depuis Cloud Run, le transfert complet, les écritures/lectures de métadonnées GCS par le worker et la gestion du verrou/reçu. Le dépassement de 10 Go a fonctionné sur ce FTP pour cette exécution ; cela ne prouve pas l'absence de tout quota. **Aucun JSON du stock n'a encore été décompressé ou normalisé, et aucun différentiel API n'a été collecté.** Les CRC internes du ZIP et les schémas des enregistrements ne sont pas encore validés.

## Matérialisation Parquet — premier membre réel validé

Code ajouté sur `feat/rne-materialization` : commande `materialize`, schéma Bronze fixe et reprise par fichier interne. Dépendances ajoutées via `uv` : `ijson`, `pyarrow`, `simplejson` (décimaux conservés sans flottants). **99 tests hors réseau réussis**, Ruff et validation OpenTofu réussis.

Le catalogue central du ZIP GCS, lu sans décompression intégrale, contient **5 470 JSON**, **177 360 918 612 octets décompressés** au total, maximum **85 269 573 octets** par fichier.

L'essai borné a été exécuté depuis le poste de développement via ADC, pas par un nouveau job Cloud Run. Il a lu uniquement `stock_000001.json`, sans retéléchargement FTP ni modification du raw :

| Information | Résultat réel |
|---|---|
| Entrée JSON | 21 574 205 octets décompressés / 1 431 155 compressés |
| Lignes acceptées | **5 000** |
| Rejets / doublons SIREN dans cette entrée | **0 / 0** |
| Parquet | **1 672 324 octets**, 20 colonnes, Zstandard |
| Fin de matérialisation UTC | 2026-10-22 18:41:00 |
| Temps interne / temps processus | 6,581 s / 8,45 s |
| CPU utilisateur + système sur Mac | 1,90 s + 0,33 s |
| RSS maximale locale | 137 084 928 octets, environ 131 MiB |
| Statut du snapshot | **partial : 1 / 5 470**, manifeste global volontairement absent |

Parquet créé :

```text
gs://industrial-intelligence-dev-data/bronze/sandbox/leo/inpi/rne/companies/snapshot_id=aebbad44755901448b9d5785cca16524dcae20852df60899f43297b205116557/schema_version=1/parts/part-000001/eb2b5901afd241e8a9421caa01d791ed.parquet
```

Manifeste d'entrée : même racine de snapshot, puis `manifests/part-000001.json`. SHA-256 du Parquet, recalculé indépendamment par relecture complète de ces 1,67 Mo depuis GCS :

```text
7a5cbd8f06f4c7e694e413a2f60baaa19c4a05bfd005971b9bd1b37f9d889a1f
```

Les 5 000 lignes ont également été relues : schéma identique, 5 000 SIREN de neuf chiffres distincts et empreinte de chaque `company_json` vérifiée. La génération/taille des deux artefacts Parquet/rejets correspondent au manifeste. Le fichier de rejets gzip vide fait 20 octets. Aucun checkpoint différentiel ni verrou résiduel ; seul le checkpoint de **matérialisation** a été avancé au membre 1.

Les valeurs des lignes et les informations personnelles n'ont pas été écrites dans les logs ou les fixtures Git. Les indicateurs de diffusion sont conservés. L'unicité SIREN nationale et la qualité des 5 469 autres membres ne sont pas encore établies. Lire les fichiers via leurs manifestes, pas par glob, pour exclure d'éventuelles tentatives orphelines.

### Image et déploiement du worker

L'image locale `industrial-intelligence-rne:materialize` a été construite en Linux/amd64 et vérifiée sans réseau/système de fichiers inscriptible : ZIP, parseur incrémental, décimaux exacts et Parquet sur une fixture synthétique. Elle a été publiée :

```text
europe-west9-docker.pkg.dev/industrial-intelligence-dev/rne/ingestion@sha256:df16dac1a83946c1eb09a01fc18271c19d6839d19d2ee88e350a704d4a2d7fed
```

Après approbation explicite du déploiement et du traitement national, `rne-materialize.tfplan` a été appliqué : **6 créations, 0 modification, 0 suppression**. Compte de service sans secret INPI, quatre autorisations GCS conditionnelles (raw lecture, bronze création/lecture, control), job `rne-materialize`, 1 vCPU / 2 GiB, sans Scheduler ni retry automatique. Le job a été vérifié `Ready=True` à **20:06:10 UTC**. Les flags/digest/révision sont persistés dans `envs/rne-dev.tfvars`. L'état local a été sauvegardé avant application dans un fichier ignoré, mode 600.

Les deux jobs de collecte restent sur leur image précédente ; le Scheduler reste en pause. Le plan suivant, avec les deux fichiers de variables, a confirmé **`No changes`**. Le défaut du job reste limité à un membre ; le lancement national utilise des overrides explicites.

### Traitement national en cours — une seule tâche

L'utilisateur a choisi de conserver une seule tâche et de n'augmenter les ressources qu'en cas de bénéfice démontré. Le code séquentiel n'a pas été parallélisé ; aucun second worker ni nouvel orchestrateur n'a été ajouté.

| Information | Valeur au lancement |
|---|---|
| Exécution | `rne-materialize-vdrpj` |
| Projet / région | `industrial-intelligence-dev` / `europe-west9` |
| Début UTC | **2026-10-22 20:10:41** |
| Configuration | **1 tâche, 1 vCPU, 2 GiB, 0 retry automatique** |
| Périmètre | **Tous les membres restants**, reprise au deuxième JSON |
| Budget d'entrée | **200 000 000 000 octets décompressés**, supérieur aux 177,36 Go connus |
| Budget souple / timeout d'exécution | **82 800 s / 86 400 s** (23 h / 24 h) |
| Limites conservées | 128 MiB par membre, lots de 500 lignes / environ 8 MiB |
| Source | ZIP GCS existant et génération épinglée, aucun nouveau transfert FTP |

La commande exacte et la lecture du statut sont dans [inpi-materialisation.md](inpi-materialisation.md). Le timeout de 24 h est limité à cette exécution ; le défaut du job reste à 6 h. Aucun plafond financier automatique n'a été créé : l'estimation annoncée avant lancement était plusieurs heures et quelques dollars de calcul hors quotas gratuits, plus stockage/opérations, à vérifier sur la facture.

**Contrôle réel à 20:15:57 UTC : 66 / 5 470 membres, 330 001 lignes valides, 0 rejet, 1 répétition locale de SIREN.** Cette répétition est conservée, pas arbitrée ; les lignes ne sont donc pas présentées comme autant d'entreprises distinctes. L'unicité nationale reste non vérifiée. Le manifeste global est encore absent : **la conversion est lancée et progresse, pas déclarée complète**.

Le premier Parquet écrit par Cloud Run (`manifests/part-000002.json`) a été relu intégralement depuis GCS : taille/SHA-256 binaire, schéma de 20 colonnes, 5 000 lignes et identifiants de neuf chiffres, empreintes de tous les JSON et provenance `stock_000002.json` / `stock` vérifiés. Cela valide les permissions du nouveau compte de service et le fonctionnement Linux/amd64 sur les données réelles, pas seulement sur une fixture.

Premiers points Cloud Monitoring disponibles : CPU moyen environ **60–65 %** d'un vCPU (20:12–20:14 UTC), mémoire environ **4,4–4,6 % des 2 GiB**, soit **90–94 MiB** (20:12–20:15 UTC). Ces observations précoces ne sont pas un maximum national, mais ne justifient pas d'allouer 2 vCPU ou davantage de RAM à ce stade.

Le verrou du namespace est présent normalement pendant cette exécution active : **ne pas le supprimer ni lancer une seconde ingestion concurrente**. Le checkpoint différentiel reste absent et le Scheduler `PAUSED`. Le guide détaille le schéma, les garanties et les limites. Attendre le manifeste global, les totaux et la fin effective du worker avant toute déclaration de complétion.

## Résultat final national — confirmé le 23 octobre

`rne-materialize-vdrpj` a terminé avec **Completed=True**, une tâche réussie, de **2026-10-22 20:10:41 UTC** à **2026-10-23 03:23:55 UTC**, soit **7 h 13 min 14 s**. Le manifeste global a été publié à **03:23:52 UTC** ; le verrou était absent au contrôle de 06:34 UTC.

Totaux du manifeste : **5 470 / 5 470 membres**, **177 360 918 612 octets décompressés**, **27 348 473 lignes d'entrée**, **27 348 472 valides**, **1 rejet**, **822 répétitions locales de SIREN**. `quality_status=requires_review` et `global_siren_uniqueness_checked=false`. La conversion est complète techniquement, pas une certification d'unicité/qualité métier.

Inventaire du bucket à **06:33 UTC** (objets vivants, hors versions/suppressions temporaires) : **32,053 Go**, dont raw **15,135 Go**, Bronze **16,205 Go**, fichier SIRENE racine **0,714 Go** ; **16 416 objets**. Les 177 Go de texte ont été décompressés en flux puis recompressés en Parquet ; le ZIP original reste intact.

## Différentiel brut + Parquet — code et déploiement, activation bloquée

Après la passe de simplification, `refresh --with-parquet` a été ajouté. La page brute et son Parquet/manifest doivent être durables **avant** l'avancement du curseur. Le checkpoint fige le mode `raw_and_parquet` ; pas de bascule silencieuse d'un historique brut déjà avancé. Une ligne invalide bloque la page entière ; tombstones et décimaux exacts sont conservés. Le détail est dans [inpi-quotidien.md](inpi-quotidien.md).

### Qualification réelle bornée

Fenêtre testée : **2026-10-21 12:00:00–12:00:10 UTC**. Le premier test strict a arrêté la validation : l'API a renvoyé des éléments exactement à la borne basse annoncée exclusive par la v5. Les diagnostics bornés n'ont pas démontré des paramètres ignorés : UTC avec/sans microsecondes et offset explicite ont donné le même count de 18. La pagination complète a obtenu **10 + 8 + 0 lignes**, dont **3 à la borne basse**, sans répétition de SIREN entre pages. Le code conserve désormais cette frontière inclusive, mais refuse toujours les timestamps strictement hors intervalle.

L'essai API → raw GCS → Parquet GCS → relecture a utilisé **5 appels API**, **18 lignes**, **31 203 octets de Parquet**. Schéma, tailles/générations, SHA-256 binaires et de chaque JSON, provenance et nombres de lignes vérifiés. Exécution depuis le poste local avec ADC/identifiants privés, sans écriture de checkpoint principal ni données personnelles dans les logs/fixtures.

```text
gs://industrial-intelligence-dev-data/control/sandbox/leo/inpi/rne/qualification/daily/20261023T072331Z-e9ebf3e3/qualification.json
```

Ce préfixe séparé est une qualification de 10 secondes, pas un stock ni un rattrapage national. Les petits objets d'essai sont privés et conservés ; aucune suppression automatique n'a été exécutée.

### Image et rollout réellement appliqués

**125 tests hors réseau réussis**, Ruff/formatage, verrou de dépendances et validation OpenTofu réussis. Linux/amd64 vérifié hors réseau : décimal exact, tombstone, schéma et reprise idempotente d'une page.

Image publiée :

```text
europe-west9-docker.pkg.dev/industrial-intelligence-dev/rne/ingestion@sha256:347bd100540d259edffa9679a66369c0073c45696abee41fcc6304eda5d66929
```

Plan relu et appliqué : **2 créations IAM conditionnelles Bronze pour `rne-refresh`, 2 mises à jour d'images des jobs de collecte, 0 suppression**. Sauvegarde locale d'état protégée avant application. Le profil active `rne_refresh_parquet=true` et configure `refresh --with-parquet --page-size 100`. `rne-bootstrap` garde son mode archive seule et n'a pas été relancé pour télécharger ; `rne-materialize` reste sur son digest précédent. Les deux jobs modifiés ont été contrôlés `Ready=True`.

Smoke Cloud Run **`rne-refresh-m5js2` réussi**, arguments **`refresh --help` seulement**, timeout 120 s. Ce test ne fait pas d'appels INPI/GCS et ne constitue pas une collecte complète par le compte de service cloud.

### Pourquoi le quotidien n'est pas encore activé

La documentation officielle consultée ne fournit pas la borne garantie de **notre** archive FTP. Ni son nom de mars ni son MDTM d'octobre ne prouvent cette couverture. `rne_initial_from` reste vide, `rne_bootstrap_archive_only=true`, le checkpoint principal absent et le Scheduler **`PAUSED`**, cron `0 5 * * *`, fuseau `Etc/UTC`.

Un garde-fou OpenTofu empêche d'activer le Scheduler si la borne est vide, si archive-only est encore configuré ou si le mode Parquet est désactivé. Il ne remplace pas la confirmation de couverture, le rattrapage ni la construction de Silver. Aucun faux checkpoint n'a été écrit pour contourner ce blocage. Le guide quotidien contient la question précise à adresser à l'INPI.

## Reconstruction API complète — préparation sans cutoff supposé

La voie temporaire « collecter seulement à partir d'aujourd'hui » n'a pas été retenue. Une reconstruction API complète est préparée ; [inpi-reconciliation.md](inpi-reconciliation.md) distingue le code disponible, les contrôles réels et les étapes encore non lancées.

### Constats qui corrigent le contrôle temporel précédent

Audit borné, sans écriture GCS : la fenêtre **2023-01-02 12:00–13:00 UTC** annonce 16 résultats, renvoie **16 SIREN distincts puis une page vide**, mais les 16 `company.updatedAt` sont entre juillet et octobre 2026. `formality.updated` est absent de 10 fiches et hors créneau pour les six autres. Le contrôle précédent sur les dates des fiches aurait donc bloqué des réponses historiques légitimes ; il a été remplacé par une validation de format, une mesure des écarts, l'unicité des pages et les counts exacts avant/après.

L'API accepte les probes avec borne basse `0001-01-01` : count **3 179** jusqu'à fin 2022 (identique à la borne 2000), **0** avant 2000. Les grandes périodes annuelles 2023–2026 et une journée récente dépassent le plafond de count de 10 000 ; cela ne fournit pas leur volume exact.

Le collecteur corrigé a été qualifié sur la fenêtre historique : **16 lignes en Parquet en mémoire, 5 appels API, count 16 avant/après, 16 écarts de timestamp conservés**, aucune écriture GCS ni checkpoint principal. La provenance/test ne prétend pas représenter un snapshot juridique à la date de recherche.

### Code et image disponibles

`init-reconciliation` vérifie uniquement les reçus/manifeste du stock GCS existant, puis peut initialiser une stratégie `full_api_reconciliation`, une borne UTC minimale et une cible gelée, avec `stock_cutoff_verified=false` et `coverage_status=rebuild_pending`. Pas de téléchargement FTP, pas de remise à zéro d'un checkpoint existant. La première collecte découpe adaptativement le grand intervalle selon le count, sans parcourir deux millénaires en tranches de six heures. Elle ne marque pas la collecte terminée avant la cible ; ensuite, elle annonce `awaiting_silver_reconciliation`, pas une base prête.

**135 tests hors réseau réussis**, Ruff, formatage et verrou de dépendances vérifiés. Image Linux/amd64 publiée et commande d'initialisation contrôlée hors réseau :

```text
europe-west9-docker.pkg.dev/industrial-intelligence-dev/rne/ingestion@sha256:341b99394fbf67cc5f4b19df6b7a98ba6bf054e60ead56e450b9d91066049632
```

Plan relu/appliqué : **0 création, 2 mises à jour d'images, 0 suppression**, aucune modification IAM supplémentaire. Les deux jobs de collecte sont `Ready=True` ; `rne-materialize` conserve son image. Sauvegarde protégée d'état avant application. Le Scheduler reste en pause.

**L'initialisation en production et le rattrapage national API n'ont pas été exécutés.** Une collecte comparable aux 27,35 millions de lignes représente environ 273 500 pages ou davantage, des jours potentiels selon quotas et de l'ordre de centaines de Go de JSON brut supplémentaire. Une enveloppe indicative **20–30 € de calcul/opérations** et plusieurs euros de stockage mensuel supplémentaire est proposée, pas autorisée par l'ancien budget du transfert FTP ni un plafond de facturation garanti.

La consolidation Silver reste à implémenter. Une table de versions et une vue courante BigQuery sont recommandées plutôt qu'une consolidation objet sur mesure, mais **aucune ressource BigQuery n'a été créée**, aucune dépendance BigQuery n'a été ajoutée et son budget doit être approuvé. Les counts stables ne prouvent pas un snapshot transactionnel fournisseur ni la rétention de tous les retraits historiques. Aucun SIREN absent n'est qualifié automatiquement de cessé.

## 24 octobre — sync GCS-only préparé, qualifié et planifié

**Choix retenu : pas de BigQuery, pas de nouveau téléchargement FTP, pas de rechargement national API d'un seul coup.** Le produit Silver est un catalogue/index courant référençant les lignes des Parquet Bronze, pas une nouvelle copie intégrale des JSON. Le récent et le backfill ont deux checkpoints/progrès distincts, sans inventer le cutoff du stock. Voir [inpi-sync.md](inpi-sync.md).

### Implémentation et tests

Modules ajoutés : `current.py` (base routée par statistiques SIREN, index API par préfixe, résolution de versions/tombstones/conflits, pointeur CAS), `sync.py` (récent prioritaire, historique borné, publication atomique). Les fenêtres acquittées forment une chaîne, excluant les tentatives orphelines. Le verrou principal est conservé durant l'orchestration, avec réentrée uniquement sur même instance/thread. Un budget souple précède le timeout dur.

**154 tests hors réseau réussis**, Ruff/formatage et validation OpenTofu réussis. Une exécution Linux/amd64 hors réseau a vérifié stock → catalogue → récent/historique → index → tombstone, avec sortie normale. Un premier essai sous émulation avait terminé son travail mais bloquait à la fermeture : les lectures des petits Parquet sur streams Python ont été rendues explicitement séquentielles via `ParquetFile.read(use_threads=False)` ; seul le conteneur de test créé par l'agent a été arrêté, aucun conteneur utilisateur existant.

Métadonnées du rejet unique du stock consultées, sans valeurs personnelles : `stock_005470.json`, ligne 2651, dépassement de l'ancienne limite normalisée de 16 MiB. La limite bornée de normalisation/page passe à **64 MiB**, avec test synthétique de 17 MiB. Le stock historique n'a pas été réécrit ; le rejet n'est pas déclaré récupéré avant observation API correspondante.

### Déploiement et permissions

Premier plan appliqué : **4 autorisations Silver de lecture/création** (comptes `rne-refresh` et `rne-materialize`), **3 mises à jour de jobs**, aucune suppression ni nouveau service. Les préfixes restent privés sous le sandbox Leo ; aucun droit de suppression/remplacement Silver/Bronze. Le pointeur mutable est sous `control/` avec CAS.

Le job quotidien reste **une tâche, un vCPU**, mémoire **2 GiB** pour les buffers d'index. Aucun parallélisme de workers, retry automatique ou nouvelle base locale. Image initiale de préparation : `sha256:4805410c0d0cc6e07d9c4f2830d1dae902f7785bc2fc3d08dc3c992ca3d275aa`.

Image finale publiée, utilisée par les trois templates de jobs après un second plan d'images seulement :

```text
europe-west9-docker.pkg.dev/industrial-intelligence-dev/rne/ingestion@sha256:89fac9a6e822f8bb575cd53a7f96c61dda6df606ccbcc0144fa968b93d235050
```

Les sauvegardes d'état et plans sont ignorés et protégés. Aucune version de secret n'a été ajoutée/modifiée.

### Préparation réelle réussie

`rne-materialize-bqqgm`, arguments `prepare-sync --stock-revision b47860c7c6a645bc1d54e878d2c4d2b5ec67b2d4530da9b1bd5f2cd2c8c9fbde` : **Completed=True**, du **24 octobre 11:12:41 UTC au 11:34:55 UTC**, **22 min 14 s**. Lectures de footers/manifestes des 5 470 Parquet, sans décompression de 177 Go ni nouvelle ingestion FTP. Catalogue disponible avec **27 348 472 lignes de base**, qualité `requires_review`, zéro observation API initiale.

`prepare-sync` a réellement créé le checkpoint principal de reconstruction (borne minimale explicite, `rebuild_pending`, cible gelée), le checkpoint du récent sous `control/.../live/`, le pointeur courant et le marqueur **`control/.../sync.json`**. Le début du flux récent est **2026-10-22 00:00 UTC** ; ce n'est **pas** un cutoff supposé du stock. Une réexécution refuse de remettre à zéro les progrès existants.

### Qualification API cloud complète sur une heure

`rne-refresh-thq84`, arguments d'exécution : `sync --until 2026-10-22T01:00:00Z --max-requests 100 --backfill-requests 0 --max-pages 80 --max-seconds 900`, timeout 1 800 s.

**Completed=True**, du **11:36:09 UTC au 11:38:01 UTC**, traitement interne **104,035 s**. Résultats : **2 586 lignes, 27 pages dont la vide terminale, 30 appels API**, checkpoint récent à **01:00 UTC**, batch terminé, catalogue courant publié avec **2 586 identifiants API indexés**. L'historique est volontairement différé dans cet essai, pas simulé comme terminé.

Relecture indépendante depuis le catalogue : résolution d'une entreprise vers sa ligne Bronze, empreinte de `company_json` recalculée/identique, sans journaliser le SIREN ou le payload personnel. Pointeur/générations, checkpoint terminé et absence des deux verrous vérifiés.

### Scheduler désormais activé

Après qualification, plan vérifié : **seule modification `paused=true → false` du Scheduler**. Le plan a lu le vrai marqueur GCS d'initialisation et vérifié sa révision avant application. État contrôlé **`ENABLED`**, cron **`0 5 * * *`**, fuseau **`Etc/UTC`**.

Arguments persistés : `sync --max-requests 5000 --backfill-requests 500 --max-seconds 16000`. Les 500 appels historiques sont **inclus dans** les 5 000, pas ajoutés ; ils ne sont utilisés que si le récent a rejoint sa cible. Quotas et délais peuvent faire durer le bootstrap historique ; cela n'est pas déclaré terminé.

Une exécution manuelle du même template, **`rne-refresh-qm62q`**, a ensuite été lancée pour poursuivre le récent restant et le backfill borné. **Elle est en cours au dernier contrôle**, pas déclarée complète. Le catalogue conserve le dernier état entièrement publié pendant ce travail. Les progrès API acquittés sont durables même si une exécution s'arrête à son budget.

**État de couverture à la qualification : `reconciling`, `api_replay_complete=false`.** Le système de collecte/lecture est opérationnel et planifié ; la vérification historique complète et les anomalies du stock ne sont pas miraculeusement résolues par l'activation. Les logs et `current-status` exposent ces distinctions.

## Simplification locale — non déployée

Le nouveau code retire `init-reconciliation`, la borne minimale de l'année 1 et les états de reconstruction intégrale. `prepare-sync` exige désormais `--delta-from` ; l'hypothèse retenue est **2026-03-01T00:00:00Z**. Le même moteur de fenêtres bornées sert au récent et au rattrapage du stock, le récent restant prioritaire.

Le marqueur sync est en **version 2**. Les checkpoints de l'ancien rejeu et marqueurs v1 sont refusés avant collecte, **pas réécrits**. La fin du rattrapage n'exclut plus les entreprises du stock absentes du différentiel. Les métadonnées `catchup_*` et `stock_cutoff_verified=false` remplacent toute déclaration de reconstruction intégrale. Voir [le contrat et la transition à préparer](inpi-sync.md).

Vérifications locales : **166 tests hors réseau**, Ruff lint/formatage, lockfile, diff whitespace, OpenTofu fmt/validate réussis. Un test fait réellement ZIP synthétique → Parquet → préparation avec borne → collecte → catalogue courant, sans GCP. Les tests des anciens modes retirés ont été remplacés par les nouveaux contrats et le refus de migrer silencieusement les contrôles.

**Aucune image construite/publiée, aucun plan/apply, aucune écriture GCS ni modification de job/Scheduler/IAM pendant cette simplification.** Le profil reste sur l'image déployée et les checkpoints existants ; il faut préparer une migration explicite avant déploiement. La précondition OpenTofu attend désormais un marqueur v2 : un plan avec le marqueur v1 actuel ne doit pas être présenté comme « sans changement, prêt à appliquer ».

Les principes demandés sont conservés dans [engineering-principles.md](engineering-principles.md), prêts à reporter manuellement dans `AGENTS.md`, resté inchangé conformément à sa règle de lecture seule.

## Deuxième passe locale — chemins d'exécution unifiés, non déployée

Mesure à périmètre identique (fichiers Python du package RNE, commentaires/espaces compris) : **12 → 9 fichiers ; 2 266 → 2 093 lignes**, soit 173 lignes retirées. Il ne s'agit pas d'une division par deux du code : le convertisseur et le catalogue courant restent nécessaires aux fonctionnalités conservées.

- Un bootstrap d'archive, sans initialisation différentielle ni flag `--archive-only`.
- Raw + Parquet systématiques ; mode raw-only, callback optionnel et conversion implicite de checkpoint supprimés.
- `RnePipeline` porte préparation, collecte et orchestration. La boucle de collecte termine le batch gelé puis le nouvel horizon dans un budget commun ; `RneSync` et sa seconde boucle sont retirés.
- `sources.py` regroupe les clients HTTP/FTP ; `companies.materialize_page()` remplace `diff.py` et sa classe mono-opération. `api.py`, `ftp.py`, `diff.py`, `sync.py` sont supprimés du code local.
- Cinq commandes seulement ; `refresh` devient une méthode interne. Quatre variables OpenTofu de variantes sont retirées, sans application du plan. Budgets, ressources, adresses des ressources et périmètres de permissions existants ne sont pas augmentés dans le profil dev.

**167 tests hors réseau passent**, lint/formatage Ruff, lockfile, diff whitespace et OpenTofu fmt/validate réussis. Les tests de modes retirés ont été remplacés par les contrats uniques ; aucune mise en skip ni désactivation de contrôle. Un test ajouté a reproduit une collision de manifeste entre batch repris et chevauchement : le chemin est désormais unique par publication de fenêtre. Un autre vérifie le budget de pages commun aux deux batches. Les tests d'idempotence comptent explicitement le raw, et non le raw plus les nouveaux Parquet.

Aucun build/push d'image, plan/apply, exécution cloud ou modification de checkpoint pendant cette passe. Les **nouveaux templates ne doivent pas être appliqués avec l'ancienne image épinglée**. Migration v2, qualification d'image et revue du plan restent nécessaires. `AGENTS.md` n'est pas modifié.

## Budget et suite

Les tarifs indicatifs consultés avant la phase 1 sont hors quotas gratuits et hors taxes : environ **0,24 USD/mois pour quatre versions actives de secrets**, plus le stockage des images. Les quotas gratuits se partagent par compte de facturation et n'ont pas été audités.

Le Scheduler créé est facturé jusqu'à **0,10 USD/mois hors quota gratuit, même en pause**. Les deux tests de démarrage et le transfert de 11 min 50 s peuvent être facturés selon les règles/minimums Cloud Run ; les durées observées ne sont pas des montants facturés. Les **15,14 Go de stockage GCS continuent d'être facturés tant que l'objet est conservé**. Aucun plafond automatique, budget ni alerte n'a été créé ; la facture réelle n'a pas été vérifiée.

Suite : suivre `rne-refresh-qm62q` puis les exécutions quotidiennes, surveiller la couverture récente/historique, les quotas, les blocages et la facture réelle. Le catalogue courant GCS et la planification sont désormais disponibles ; ne pas reconfigurer une fausse borne du stock ni relancer une initialisation sur les checkpoints existants. La qualité du stock (rejet/répétitions), la couverture historique complète, une éventuelle récupération du gros objet et la rétention/purge des anciennes versions restent à vérifier. Les budgets applicatifs ne plafonnent pas la facture. La voie ancienne avec cutoff vérifié reste une alternative explicite, pas une modification à appliquer au mode sync opérationnel.
