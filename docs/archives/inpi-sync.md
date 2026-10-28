> **Obsolète (28 octobre 2026) :** le catalogue courant, `prepare-sync` et `current-status` ont été retirés. Le fonctionnement actuel est décrit dans [inpi-ingestion.md](../rne/inpi-ingestion.md). Cette page est conservée comme historique.

# RNE — stock, rattrapage et quotidien

**Ce guide décrit le code local simplifié, pas une migration cloud déjà effectuée.** Les images et checkpoints déployés utilisent encore l'ancien rejeu intégral. Voir le [journal des opérations réelles](deploiement-rne-dev.md).

## Une seule méthode de collecte

```text
Stock Bronze existant + pages API raw/Parquet
                       ↓
            catalogue courant GCS
```

Pas de BigQuery, pas de nouveau téléchargement FTP, pas de copie quotidienne du stock. Le catalogue Silver référence les lignes Bronze par objet/génération/index ; les JSON restent dans les originaux immuables.

`RnePipeline.refresh()` est le moteur commun (interne, pas une commande CLI) : fenêtres de 6 h réduites selon le count, pagination complète, raw/Parquet avant checkpoint. `sync` organise deux progrès pendant le rattrapage pour que la base continue à recevoir les changements récents :

- **Récent prioritaire** : `control/{prefix}/live/checkpoint.json`.
- **Rattrapage du stock** : `control/{prefix}/checkpoint.json`, stratégie `stock_catchup`. Sa cible gelée est le début du récent ; il ne rejoue pas les années antérieures à la borne choisie.

Une fois le rattrapage terminé, seules les mises à jour récentes sont collectées. La fin est déduite du watermark et de l'absence de batch en attente : pas de machine à états de reconstruction intégrale. `RnePipeline.prepare_sync()` et `RnePipeline.sync()` remplacent la classe et le module `RneSync`/`sync.py`.

## Préparation explicite

Après validation du stock et du budget de préparation :

```sh
# Exemple de la nouvelle interface ; ne pas lancer sur les checkpoints v1 déployés.
uv run rne-ingest --bucket industrial-intelligence-dev-data \
  --prefix sandbox/leo/inpi/rne prepare-sync \
  --stock-revision b47860c7c6a645bc1d54e878d2c4d2b5ec67b2d4530da9b1bd5f2cd2c8c9fbde \
  --delta-from 2026-03-01T00:00:00Z
```

La borne est obligatoire, pas inférée du nom du ZIP ou d'un `updatedAt`. **Le 1er mars 2026 est l'hypothèse retenue**, pas une couverture certifiée par l'INPI ; voir [les indices du ZIP et les limites](../rne/inpi.md#recherche-publique-complémentaire-et-datation-interne-du-zip).

`prepare-sync` vérifie le stock complet, prépare son routage par footers et initialise les deux checkpoints. Le récent commence à minuit UTC deux jours avant la préparation, sans descendre avant la borne choisie. Le marqueur `control/{prefix}/sync.json` est en **version 2** et conserve `stock_assumed_from`.

Une relance identique conserve les progrès. Une ancienne stratégie, un marqueur v1 ou une borne différente sont refusés : aucune remise à zéro silencieuse. La commande `init-reconciliation` a été supprimée.

## Collecte et lecture

- `sync` : récent, puis rattrapage dans sa réserve, puis publication du catalogue.
- `current-status` : disponibilité, compteurs et couverture, sans payload personnel.
- `bootstrap` : archive seulement ; `materialize` : convertit le stock. Les commandes `refresh`/`init-reconciliation` et options `--archive-only`/`--with-parquet` sont supprimées. Raw + Parquet est systématique.

Limites inchangées : **5 000 appels au total dont au plus 500 pour le rattrapage**, 4 500 pages par phase, 16 000 s souples de collecte, 100 entreprises/page et 64 MiB/page. Login/counts/retries sont compris. Ce ne sont pas des plafonds financiers.

L'horizon quotidien reste minuit UTC moins un jour ; chevauchement de 24 h. Un batch en attente garde sa cible et son curseur. Une erreur du rattrapage n'empêche pas la publication des changements récents déjà validés.

Le lecteur autorisé `CurrentCompanies.lookup(siren)` privilégie les observations API ; un tombstone retourne `deleted`, sans payload. **Sans observation API, le stock reste la référence, même après la fin du rattrapage.** L'absence dans le différentiel ne signifie pas qu'une entreprise a disparu. Les conflits de payload à timestamps égaux sont signalés, pas arbitrés.

## Publication et protections conservées

Les manifestes de fenêtres forment une chaîne `previous_window_manifest`. Le catalogue ne consomme que ces fenêtres terminées, pas un glob de tentatives. Il lit les colonnes de routage, écrit des index Parquet par préfixe SIREN, puis publie un pointeur avec précondition de génération.

```text
silver/{prefix}/companies/base/{archive_sha256}/manifest.json
silver/{prefix}/companies/indexes/prefix=NNN/{attempt}.parquet
silver/{prefix}/companies/catalogs/{attempt}.json
control/{prefix}/current.json
control/{prefix}/sync.json
```

Le verrou principal couvre tout le sync ; pas d'expiration permettant de voler le verrou d'un job actif. Les streams Parquet restent épinglés à une génération, avec lectures Arrow séquentielles. Originaux privés et immuables, provenance et restrictions de diffusion conservées. Aucun nettoyage destructif automatique.

## Couverture : ne pas confondre fini et certifié

`current-status` conserve les compteurs de base/index et expose dans `coverage` :

- `catchup_from`, `catchup_until`, `catchup_target`, `catchup_complete` ;
- `live_from`, `live_until`, `backfill_status` et éventuelle `backfill_error` ;
- `status=catching_up` ou `caught_up` ;
- **`stock_cutoff_verified=false`**, même lorsque le périmètre choisi est entièrement parcouru.

`caught_up` signifie « périmètre de collecte demandé terminé », pas exactitude juridique absolue ou historique complet depuis 2023. Les anomalies de qualité du stock restent déclarées. Le produit est un état courant ; voir [la qualification distincte des états historiques](../rne/inpi-historique.md).

## Transition cloud encore nécessaire

Ce changement n'est **pas compatible en écriture avec les checkpoints de reconstruction v1**. Le code refuse de les réinterpréter ; OpenTofu exige désormais le vrai marqueur de préparation v2 pour activer le mode sync.

Avant déploiement : établir un plan de migration préservant raw/Bronze, catalogue, anciennes chaînes de fenêtres et progrès récents ; sauvegarder les contrôles avec leurs générations ; chiffrer le rattrapage depuis mars ; qualifier la nouvelle image ; puis appliquer une transition approuvée. Ne pas relancer aveuglément `prepare-sync`, modifier à la main le watermark pour « finir » le rattrapage, ou appliquer le profil d'infrastructure actuel en supposant le marqueur déjà migré.

Aucun `tofu apply`, changement IAM/Scheduler ou écriture GCS n'a été effectué pour cette simplification. Le profil reste épinglé à l'image déjà déployée. Les variables de variantes raw-only/bootstrap initialisant le différentiel sont supprimées ; les templates ont désormais une recette unique. Il faut donc publier/qualifier la nouvelle image avant toute application. Les budgets et ressources (une tâche, un vCPU, 2 GiB pour le job sync) ne sont pas augmentés.
