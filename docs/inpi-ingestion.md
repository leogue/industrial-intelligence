# Ingestion RNE : stock FTP et différentiel quotidien

## 1. Périmètre et état réel

Implémentation Python 3.13, une image, deux commandes :

- **`bootstrap`** : copie un ZIP explicite de **formalités entreprises** du FTP vers GCS, en flux ; calcule sa taille et son SHA-256, écrit un manifeste, puis initialise un checkpoint.
- **`refresh`** : récupère les réponses complètes de `/companies/diff`, avec pagination, provenance et reprise ; stocke les octets JSON reçus, sans les transformer.

**Ce n'est pas encore l'ETL** : le ZIP n'est ni décompressé ni converti en fiches par entreprise. Les pages API sont des tableaux `[{"company": {...}}]`, pas des patches à appliquer au ZIP. Construire l'état courant par SIREN et les tables Silver sera une étape séparée. Aucun PDF, bilan saisi, SFTP documentaire, OCR ou graphe n'est ingéré par cette version.

Le choix de conserver des pages API, plutôt qu'un objet par SIREN/version, limite le nombre de petites écritures GCS et facilite la reprise. Tous les champs restent dans le brut. Une empreinte de page ne remplace pas une comparaison métier des entreprises.

**Rien n'est présumé déployé.** OpenTofu définit des ressources opt-in ; les valeurs par défaut ne créent aucun nouveau service RNE. Aucun téléchargement national ou test réel INPI/GCS n'est nécessaire pour exécuter les tests unitaires. Voir aussi [l'exploration de la source](inpi.md).

## 2. Organisation du code

```text
src/industrial_intelligence/ingestion/rne/
├── cli.py       # commandes, paramètres et variables d'environnement
├── api.py       # login, GET, backoff, limites, pagination et count
├── ftp.py       # SIZE/MDTM et transfert borné en mémoire
├── storage.py   # GCS, créations immuables et préconditions de génération
├── pipeline.py  # bootstrap, fenêtres, manifestes, checkpoints et verrou
└── common.py    # dates UTC, validation minimale et erreurs sans secrets
```

Dépendances d'exécution : `httpx`, `google-cloud-storage`. Tests : `pytest` ; qualité : Ruff. Les versions résolues sont dans `uv.lock`. Pas de nouvelle infrastructure locale ni de service réseau par connecteur.

## 3. Stockage

Exemple avec `RNE_PREFIX=inpi/rne` :

```text
raw/inpi/rne/
├── stocks/{remote_revision}/{run_id}/
│   ├── stock_RNE_formalites_....zip
│   └── stock_RNE_formalites_....zip.manifest.json
└── companies/from={datetime_utc}/to={datetime_utc}/
    ├── {sha256_response}.json
    └── manifest-{run_id}.json

control/inpi/rne/
├── checkpoint.json
├── lock.json                         # présent seulement pendant une exécution
└── stocks/{remote_revision}.json      # reçu d'archive réutilisable après incident
```

- `remote_revision` = empreinte du **nom + taille + MDTM**, pas hash de contenu ni date métier.
- SHA-256 du ZIP calculé sur tous les octets transférés. La signature ZIP et les tailles sont vérifiées ; **pas de validation intégrale des CRC internes ni du schéma des enregistrements du stock** à cette étape.
- GCS reçoit le ZIP en upload résumable, avec un tampon de **8 MiB** ; les blocs FTP font 1 MiB. Aucun fichier de 15 Go sur le disque/mémoire du conteneur.
- Le producteur vérifie le succès FTP, la taille, la signature et la stabilité SIZE/MDTM **avant de finaliser** l'upload GCS. Une erreur dans le contexte du writer annule l'upload résumable. Aucun reçu ni checkpoint ne valide un transfert partiel.
- Originaux créés avec `if_generation_match=0`, jamais remplacés. Les réponses API identiques d'une même fenêtre retrouvent le même nom par hash.
- Un objet dont l'upload est terminé mais qui n'a pas de manifeste/reçu après un crash reste **orphelin, pas une ingestion validée**. Pas de nettoyage automatique destructif.
- Les manifestes des fenêtres terminées contiennent les URI/hashes/tailles/dates de récupération et curseurs de toutes les pages, y compris la page vide finale.
- Pendant une fenêtre incomplète, ces références sont dans le checkpoint ; les consommateurs doivent utiliser les manifestes terminés, pas supposer que tout objet visible est une fenêtre complète.

Pour les essais, choisir un préfixe personnel, par exemple `sandbox/alice/inpi/rne`, qui produit `raw/sandbox/alice/inpi/rne/` et `control/sandbox/alice/inpi/rne/`. Ne pas utiliser le checkpoint de l'équipe pour un test.

## 4. Installer et vérifier sans réseau métier

```sh
uv sync --locked
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run rne-ingest --help
uv run rne-ingest bootstrap --help
uv run rne-ingest refresh --help
```

Les tests bloquent les connexions socket et utilisent un stockage en mémoire, des mocks GCS/FTP et `httpx.MockTransport`. Ils couvrent notamment les erreurs, les tombstones enveloppés comme entreprises lorsqu'ils respectent le contrat minimal, les curseurs, la reprise, les générations, les bornes et la stabilité du stock. Ils ne prouvent pas les performances nationales ni les droits réels GCP.

## 5. Configuration locale et exécution manuelle

Les secrets sont lus dans l'environnement. `.env` est ignoré par Git ; chargement **explicite** via `uv run --env-file .env`. Copier `.env.example` seulement si aucun `.env` privé n'existe déjà, puis `chmod 600 .env`. Ne pas écraser un fichier privé existant.

| Variable | Usage |
|---|---|
| `RNE_BUCKET` | Nom du bucket, sans `gs://` ; ADC pour l'accès GCS |
| `RNE_PREFIX` | Namespace, défaut `inpi/rne` ; préférer un namespace personnel en développement |
| `RNE_STOCK_FILE` | Nom exact du ZIP de formalités ; aucun choix automatique de « dernier stock » |
| `RNE_INITIAL_FROM` | Borne de rattrapage explicite avec fuseau ; aucune date implicite |
| `INPI_FTP_HOST`, `INPI_FTP_PORT` | Défauts `www.inpi.net`, `21` |
| `INPI_FTP_USERNAME`, `INPI_FTP_PASSWORD` | Couple technique FTP, pas les identifiants personnels |
| `INPI_USERNAME`, `INPI_PASSWORD` | Compte personnel pour l'API RNE |
| `INPI_API_BASE_URL` | Seule l'origine de production documentée est autorisée dans cette version |

Les variables SFTP de l'exemple ne sont pas utilisées par ce connecteur.

### Bootstrap — action de masse à approuver séparément

La date du nom du stock et MDTM **ne suffisent pas** à déterminer la date de couverture. Il faut choisir une borne suffisamment ancienne, vérifier la disponibilité des deltas et documenter cette décision avant de prétendre à l'exhaustivité. Le code impose une borne explicite, mais **ne peut pas certifier sa validité métier**.

Après validation du bucket, du préfixe, du volume et du budget, renseigner `RNE_STOCK_FILE` et `RNE_INITIAL_FROM`, puis :

```sh
# ATTENTION : télécharge le stock sélectionné et écrit dans GCS.
uv run --env-file .env rne-ingest \
  --bucket industrial-intelligence-dev-data \
  --prefix sandbox/alice/inpi/rne \
  bootstrap --allow-large-download
```

- Sans `--allow-large-download`, un stock de plus de **100 MiB** est refusé.
- Limite absolue par défaut : **20 GiB**, ajustable avec `--max-stock-bytes`.
- Un checkpoint déjà initialisé n'est jamais réinitialisé par bootstrap.
- Après un crash entre le reçu du ZIP et l'initialisation du checkpoint, le reçu permet de réutiliser l'archive si taille/génération GCS et hôte FTP correspondent.
- Un nouveau bootstrap/snapshot doit utiliser un **nouveau namespace**, avec une procédure de bascule explicite ; ne pas effacer le checkpoint courant pour « rafraîchir » le stock.

### Différentiel

```sh
# Nécessite un bootstrap terminé dans le même namespace.
uv run --env-file .env rne-ingest \
  --bucket industrial-intelligence-dev-data \
  --prefix sandbox/alice/inpi/rne \
  refresh
```

Valeurs par défaut :

- horizon = **minuit UTC du jour courant moins un jour** (`--lag-days 1`) ; on ne collecte pas une journée encore en cours ;
- fenêtres de **6 h**, réduites de moitié si `/companies/diff/count` annonce >10 000 résultats ; arrêt si une fenêtre d'une seconde est encore trop grande ;
- recouvrement de **24 h** depuis le watermark précédent, sans descendre avant la borne initiale ;
- **10 entreprises/page**, au plus **4 500 pages** et **5 000 requêtes HTTP** par exécution (login et retries compris) ;
- réponse limitée à **64 MiB** ; si une page dépasse cette limite, arrêt sans checkpoint de page, puis relance explicite avec `--restart-window --page-size 5` pour rejouer la fenêtre incomplète depuis son début ;
- espacement minimal de 0,25 s par requête, cinq tentatives sur erreurs transitoires, backoff et `Retry-After` jusqu'à 300 s. Au-delà, pause sans réessayer trop tôt ;
- une expiration `401` autorise une reconnexion ; `403`, redirection, JSON invalide ou schéma inattendu ne sont pas réessayés aveuglément.

Une fenêtre n'est terminée qu'après une **page vide explicite**. Un curseur manquant/répété sur une page non vide ou un `updatedAt` hors fenêtre fait échouer le job : aucun résultat n'est silencieusement ignoré. Un SIREN reste une chaîne de neuf chiffres, avec contrôles de cohérence entre racine et formalité.

Le watermark n'avance qu'après les écritures réussies. Une interruption reprend la **fenêtre, la taille de page et le curseur enregistrés**, pas une nouvelle fenêtre recalculée. L'option manuelle `--restart-window` permet de réinitialiser uniquement la fenêtre incomplète pour la rejouer (curseur invalidé ou changement de taille de page), sans avancer le watermark ni effacer les originaux. Un batch interrompu conserve son horizon ; l'exécution suivante le termine avant d'établir un horizon plus récent. Le recouvrement est rejoué une seule fois par batch, pas à chaque page. Il réduit certains risques de retard sans garantir la capture des corrections très tardives.

L'API doit continuer d'honorer ses fenêtres et curseurs pour que la reprise soit complète. Pas de snapshot transactionnel garanti côté INPI : une réconciliation périodique reste à concevoir.

Codes de sortie : **0** terminé/à jour ; **2** budget/pause avec progression conservée ; **1** erreur. Une pause n'est pas un succès silencieux. Aucun compteur ne coordonne les quotas entre plusieurs namespaces ou l'IHM : les budgets par exécution ne garantissent pas de rester sous le quota quotidien partagé.

## 6. Construire l'image

`Dockerfile.rne` contient une seule image d'exécution Python 3.13, non-root, sans outils de développement. Les dépendances sont installées depuis `uv.lock`. Le contexte Docker est une **allowlist** : `.env`, credentials GCP, `.git`, états/plans OpenTofu et tests n'y entrent pas.

```sh
# Nécessite un moteur Docker disponible ; aucune infrastructure locale à déployer.
docker buildx build --platform linux/amd64 --load -f Dockerfile.rne \
  -t industrial-intelligence-rne:dev .
docker run --rm --platform linux/amd64 --network none --read-only \
  industrial-intelligence-rne:dev --help
```

Les bases Python/uv sont versionnées par tag dans le Dockerfile ; reconstruire à une autre date peut changer la base. Le déploiement exige ensuite le **digest de l'image produite**, pour ne pas changer de binaire lors d'une nouvelle exécution. Le même digest est utilisé par les deux jobs.

## 7. OpenTofu : ressources préparées, non automatiquement déployées

Fichiers : `infra/opentofu/rne.tf`, `rne_variables.tf`. Le bucket existant n'est ni renommé ni déplacé. Région proposée : **`europe-west9`**, distincte de la localisation multi-région `EU` du bucket.

### Ressources définies

- Dépôt Artifact Registry Docker `rne`.
- Quatre **conteneurs de secrets vides** : identifiants/mots de passe API et FTP. **Aucune valeur secrète dans OpenTofu ou l'état.**
- Deux service accounts d'exécution, chacun accédant seulement à ses deux secrets.
- Accès GCS conditionnels par préfixe : création/lecture sur `raw/PREFIX/`, manipulation des checkpoints/verrous sur `control/PREFIX/`. Aucun droit de suppression/remplacement du brut ; pas de droit de lister tout le bucket.
- Deux Cloud Run Jobs, **1 tâche, parallélisme 1, 1 vCPU / 1 GiB**, sans retries automatiques :
  - `rne-bootstrap` : `bootstrap --allow-large-download`, timeout **24 h**, exécution manuelle ;
  - `rne-refresh` : `refresh`, timeout **6 h**.
- Un compte Scheduler avec `roles/run.invoker` sur **le seul job refresh**.
- Un Scheduler **quotidien à 05:00 UTC**, POST OAuth sur l'API Cloud Run `:run`, **en pause par défaut**.
- Activation explicite des API nécessaires lorsque les flags sont activés ; rien n'est activé dans les valeurs par défaut.

Les secrets sont protégés par `prevent_destroy` et les jobs par `deletion_protection`. Désactiver les flags après déploiement n'est pas une procédure de nettoyage ; vérifier tout plan et les protections avant une destruction approuvée.

### Déploiement en étapes — après approbation du plan et du budget

Toutes les commandes ci-dessous sont des **actions manuelles proposées, pas des actions déjà effectuées**. Ne pas lancer OpenTofu simultanément à plusieurs, l'état reste local.

**A. Préparer le registre et les secrets vides** : dans les variables d'environnement OpenTofu, définir :

```hcl
enable_rne_prerequisites = true
enable_rne_ingestion     = false
rne_schedule_enabled    = false
```

Puis `tofu init`, `tofu fmt -check`, `tofu validate`, `tofu plan -var-file=envs/dev.tfvars -out=dev.tfplan`. Faire approuver ce plan avant tout `tofu apply dev.tfplan`.

**B. Publier l'image et ajouter les versions de secrets**, avant de créer les jobs :

```sh
# Depuis la racine ; nécessite les droits de publication, sans clé de SA.
gcloud auth configure-docker europe-west9-docker.pkg.dev
IMAGE=europe-west9-docker.pkg.dev/industrial-intelligence-dev/rne/ingestion:dev
docker build --platform linux/amd64 -f Dockerfile.rne -t "$IMAGE" .
docker push "$IMAGE"
gcloud artifacts docker images describe "$IMAGE" --format='value(image_summary.digest)'
```

Reporter le digest obtenu dans `rne_image`, avec le chemin sans tag : `.../rne/ingestion@sha256:...`. Les secrets se transmettent par stdin, pas dans les arguments shell. Exemple manuel, à exécuter seulement après création des conteneurs de secrets :

```sh
uv run --env-file .env python - <<'PY'
import os
import subprocess

mapping = {
    "inpi-rne-api-username": "INPI_USERNAME",
    "inpi-rne-api-password": "INPI_PASSWORD",
    "inpi-rne-ftp-username": "INPI_FTP_USERNAME",
    "inpi-rne-ftp-password": "INPI_FTP_PASSWORD",
}
# Valider toutes les valeurs avant le premier appel.
values = {secret: os.environ[variable] for secret, variable in mapping.items()}
assert all(values.values()), "Missing secret value"
for secret, value in values.items():
    subprocess.run(
        ["gcloud", "secrets", "versions", "add", secret,
         "--project=industrial-intelligence-dev", "--data-file=-"],
        input=value.encode(), stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE, check=True,
    )
print("Secret versions added; no values printed")
PY
```

`gcloud` et les SDK ADC peuvent utiliser des identités différentes. Le compte qui publie/applique doit disposer des droits nécessaires, notamment pour attacher les service accounts ; aucun rôle Owner/Editor n'est ajouté par commodité. Les secrets sont référencés par version `latest` : une nouvelle version sera utilisée lors des exécutions suivantes, sans inscrire sa valeur dans le job.

**C. Créer les jobs, Scheduler toujours en pause** :

```hcl
enable_rne_prerequisites = true
enable_rne_ingestion     = true
rne_image               = "<chemin image>@sha256:<digest réel de 64 caractères>"
rne_stock_file          = "<nom ZIP confirmé>"
rne_initial_from        = "<borne ISO 8601 vérifiée avec fuseau>"
rne_schedule_enabled    = false
```

Refaire `plan`, revue puis application approuvée. Les jobs ne sont pas exécutés par `apply`.

**D. Après autorisation du téléchargement**, lancer manuellement le bootstrap :

```sh
gcloud run jobs execute rne-bootstrap \
  --project=industrial-intelligence-dev --region=europe-west9 --wait
```

Contrôler son succès, le manifeste et le checkpoint. Lancer ensuite `rne-refresh` de la même façon pour le rattrapage, en respectant le quota partagé. Une grande période de rattrapage peut demander plusieurs exécutions/jours. Ne pas activer la planification si le débit de traitement ne rattrape pas les nouveaux changements.

**E. Activer ensuite seulement** `rne_schedule_enabled = true`, avec nouveau plan approuvé.

## 8. Reprise et exploitation

- Un seul verrou GCS commun au bootstrap et au refresh du namespace. La création et la suppression du verrou, ainsi que les écritures de checkpoint, utilisent des **préconditions de génération**.
- Le verrou est supprimé sur une sortie Python normale, y compris les erreurs gérées. Un arrêt brutal/timeout peut le laisser présent. **Pas d'expiration automatique**, pour ne pas voler le verrou à un job encore actif.
- En cas de verrou abandonné : vérifier les exécutions Cloud Run, confirmer qu'aucun worker n'est actif, sauvegarder/inspecter le checkpoint, puis faire approuver la suppression **du seul verrou**. Ne pas supprimer automatiquement le checkpoint ou les originaux.
- Un transfert FTP interrompu n'a **pas de reprise par offset persistée** dans cette première version. L'upload GCS est résumable pendant le processus, mais un nouveau processus recommence le transfert si aucun reçu complet n'existe. Ni des octets partiels ni leur hash ne sont déclarés complets.
- Les erreurs source/SDK ne journalisent ni réponse personnelle, ni mot de passe, ni JWT. Les logs de sortie contiennent un statut et des compteurs ; les exceptions SDK inconnues sont réduites au nom de leur classe.
- Surveiller séparément le **succès des exécutions Cloud Run**, l'âge de `completed_until`, les pauses (code 2), les verrous anciens et les volumes. Une réponse HTTP positive à Scheduler signifie seulement « demande d'exécution acceptée ».
- Les alertes/budgets et une réconciliation exhaustive ne sont **pas déployés par cette configuration** ; à définir avant exploitation régulière.

## 9. Coûts, limites et étapes suivantes

Hypothèse demandée : le FTP permet le téléchargement du stock sans quota bloquant. **Cette hypothèse n'est pas une garantie INPI.** Les quotas documentés API/SFTP et leurs différences sont détaillés dans `inpi.md`.

Dimensionnement initial, pas un devis :

- Stock entreprises observé : **15,14 Go compressés**, plus manifestes ; aucun stock de comptes de 4,30 Go ajouté ici.
- Conserver plusieurs tentatives orphelines peut multiplier ce volume ; aucun cycle de rétention ne supprime automatiquement le brut.
- Avec 20 000 changements/jour et 10 résultats/page : environ 2 000 pages nouvelles, auxquelles s'ajoutent recouvrement, pages vides et requêtes count. Taille des fiches très variable : volume réseau et stockage quotidien à mesurer.
- Le job bootstrap est plafonné à **24 vCPU-heures et 24 GiB-heures** par exécution à ressources constantes ; le refresh à **6 vCPU-heures et 6 GiB-heures**. Ce sont des plafonds de durée configurée, pas des consommations attendues ou des prix.
- Prévoir stockage GCS, opérations GCS de contrôle/écriture par page, image Artifact Registry, versions/appels Secret Manager, Scheduler et logs. Confirmer les prix régionaux, le budget et les alertes avant déploiement.

Avant d'annoncer une ingestion nationale fiable : vérifier couverture/date du stock, contrat des bornes et curseurs, droits réels Secret Manager/GCS/Cloud Run, débit/mémoire sur un essai autorisé et absence de trous au passage stock → deltas. Les droits et protections n'ont pas été validés par une exécution cloud pendant les tests unitaires.

Étapes suivantes : traitement incrémental des tableaux JSON du ZIP, normalisation/déduplication par SIREN avec provenance, gestion analytique des cessations/retraits et règles de non-diffusion, rapprochement SIRENE, puis bilans structurés. Les suppressions réglementaires exigent une procédure séparée et autorisée ; l'immuabilité technique du brut ne prime pas sur les obligations de retrait.

## 10. Vérifications réalisées pour cette implémentation

- `uv run pytest -q` : **58 tests réussis**, connexions réseau bloquées.
- `uv run ruff check .` et `uv run ruff format --check .` : réussis.
- `uv lock --check` et `uv build --wheel` : réussis ; package RNE présent dans le wheel, sans `.env` ni état OpenTofu.
- Aides des deux commandes exécutées ; `tofu fmt -check` et `tofu validate` réussis, sans plan/apply.
- Image locale **`industrial-intelligence-rne:dev` construite en `linux/amd64`** via Buildx depuis le Mac ARM, après démarrage de Docker. Aides générale/bootstrap/refresh et imports d'exécution testés sous émulation, réseau désactivé et système de fichiers en lecture seule.
- Runtime vérifié : architecture x86_64, utilisateur `10001:10001`, extension native CRC32C fonctionnelle ; pas de pytest/Ruff, `.env`, `.git` ou répertoire `infra` dans `/app`. Les 58 tests hors réseau ont été réexécutés avec succès sur le poste hôte ; ils n'ont pas été exécutés dans le conteneur.
- Image non publiée dans Artifact Registry ; aucune validation d'ingestion réelle ou de déploiement Cloud Run à revendiquer.
- Aucun appel métier INPI/GCS, déploiement, changement IAM, ajout de valeur Secret Manager ou téléchargement national exécuté pendant cette implémentation.
