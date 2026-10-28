# RNE — historique de préparation des mises à jour quotidiennes

> **Mise à jour du 24 octobre : le mode opérationnel est désormais `sync`, documenté dans [inpi-sync.md](inpi-sync.md).** Le catalogue courant GCS est préparé, un essai API cloud complet a réussi et le Scheduler est activé à 05:00 UTC. Les paragraphes du 23 octobre ci-dessous décrivent l'étape précédente ; la couverture historique reste explicitement en réconciliation.

## État réel au 23 octobre 2026

- Le stock national est converti : **5 470 fichiers, 27 348 472 lignes valides**, 1 rejet et 822 répétitions de SIREN dans les fichiers. L'unicité nationale n'est pas encore vérifiée.
- `rne-refresh` est déployé avec `refresh --with-parquet --page-size 100`.
- **Le Scheduler reste `PAUSED`**, configuré pour `05:00 UTC`. Aucun checkpoint principal n'a été initialisé. Une [reconstruction API complète](inpi-reconciliation.md) est désormais préparée pour ne pas dépendre du cutoff inconnu du ZIP ; son lancement national exige un budget distinct et la consolidation Silver reste à réaliser.
- La qualification réelle API → GCS → Parquet a porté sur **18 enregistrements**, dans un namespace séparé, pas sur un rattrapage national.
- **Pas encore de table Silver d'état courant** : les mises à jour sont des versions supplémentaires, elles ne réécrivent pas le snapshot.

## Fonctionnement du job

```text
Checkpoint principal (borne confirmée + fenêtre/curseur en attente)
    → API /companies/diff/count : découper les fenêtres sous 10 000 résultats
    → API /companies/diff : suivre le curseur jusqu'à une page vide
    → GCS raw : réponse JSON originale, adressée par SHA-256
    → GCS bronze : Parquet à schéma fixe v1, JSON complet et provenance
    → manifeste Bronze de la page
    → checkpoint : avancer le curseur uniquement après les deux sauvegardes
    → manifeste de fenêtre : toutes les pages acquittées, dont la page vide finale
```

Les fenêtres font au plus 6 h, avec recouvrement de 24 h entre batches. Leurs bornes sont gelées pendant la reprise. La borne finale par défaut est minuit UTC moins un jour de marge : un lancement le 23 octobre à 05:00 UTC vise donc le 22 octobre à 00:00 UTC, pas les changements du jour en cours.

Les budgets restent bornés : 4 500 pages et 5 000 requêtes HTTP par exécution (login/retries compris), environ quatre requêtes/s au maximum, et aucune relance Cloud Run automatique. Le job déployé demande 100 entreprises/page ; un budget atteint conserve le progrès, sans déclarer tout le batch terminé. HTTP 200 du Scheduler confirme la création d'une exécution, pas sa réussite.

### Bornes observées, différentes du contrat documenté

La [documentation INPI formalités v5](https://www.inpi.fr/sites/default/files/2026-08/documentation%20technique%20API%20formalite%CC%81s_v5.0-AA.pdf) annonce `from` exclusif et `to` inclusif. Sur la fenêtre du **21 octobre 2026, 12:00:00–12:00:10 UTC**, l'API a renvoyé **18 enregistrements, dont 3 exactement à la borne basse**, en deux pages non vides puis une page vide.

L'audit historique complémentaire a ensuite montré 16 fiches datées de 2026 pour une fenêtre de 2023. Le contrôle initial de plage sur `company.updatedAt` était donc incorrect : **les dates des fiches sont validées et les écarts audités, mais ne filtrent plus les réponses**. La clôture exige maintenant pagination vide, unicité des SIREN par fenêtre et égalité des counts avant/après. Voir [la qualification historique](inpi-reconciliation.md). Le champ de manifeste `from_exclusive` décrit le contrat demandé ; `lower_bound_included_when_returned=true` précise la tolérance observée. Les répétitions entre fenêtres/chevauchements doivent être dédupliquées dans Silver, pas supprimées arbitrairement du brut.

### Sorties et reprise

```text
raw/{prefix}/companies/from=.../to=.../{sha256}.json
raw/{prefix}/companies/from=.../to=.../manifest-{run_id}.json

bronze/{prefix}/companies/changes/page_id={sha256_du_chemin_raw}/schema_version=1/
    parts/{attempt_id}.parquet
    manifest.json
```

Chaque entrée `pages` non vide du manifeste de fenêtre référence son manifeste Bronze dans `bronze`. La page vide finale ne crée pas de Parquet inutile. Lire ces références, **pas un glob**, car une interruption peut laisser une tentative orpheline.

Le normaliseur conserve les décimaux exacts, le JSON complet de l'entreprise, `source_kind=diff`, les identifiants et les tombstones `deleted=true`. Une ligne différentielle invalide bloque **toute la page** : contrairement au bootstrap, elle n'est pas rejetée tout en avançant le curseur. Si l'échec survient pendant cette normalisation après archivage, le corps brut reste disponible pour investigation. Les réponses rejetées plus tôt par la validation API ne sont ni acquittées ni garanties archivées. Les messages d'erreur ne contiennent pas les valeurs personnelles.

La normalisation limite une page à **100 lignes / 16 MiB bruts**, en plus de la limite existante par entreprise. Si la page dépasse cette limite, réduire la taille de page ; pour une fenêtre déjà commencée, la taille de page est gelée, il faut une reprise explicite `--restart-window`. Cette limite borne le volume d'entrée, pas le pic mémoire d'un objet pathologique.

Une reprise réutilise le Parquet déjà committé si le checkpoint a échoué ensuite. Elle vérifie la génération/taille du brut et du Parquet. Les générations GCS et les préconditions de création empêchent l'écrasement silencieux.

Les nouveaux checkpoints de fenêtre conservent aussi le count attendu et les SIREN déjà acquittés (au plus 10 000), pour contrôler la pagination complète, même après redémarrage. Une ancienne fenêtre sans ces champs demande un `--restart-window` explicite.

Le checkpoint fige `output_mode=raw_and_parquet`. Le mode brut seul ne peut pas reprendre ce checkpoint. Inversement, des fenêtres déjà collectées en brut ne sont pas automatiquement considérées comme matérialisées : leur migration/backfill doit être explicite. Une initialisation encore vierge peut passer au mode brut + Parquet.

## Qualification réellement effectuée

Exécution locale via ADC et identifiants API privés, sorties GCS isolées :

```text
gs://industrial-intelligence-dev-data/control/sandbox/leo/inpi/rne/qualification/daily/20261023T072331Z-e9ebf3e3/qualification.json
```

Résultats : **5 appels API**, 3 pages (dont la finale vide), **18 lignes**, **31 203 octets de Parquet**. Les deux Parquet ont été relus : tailles/SHA-256 binaires, schéma, nombre de lignes, empreintes JSON et provenance contrôlés. Aucun SIREN répété entre ces pages. Le checkpoint principal était absent avant et après l'essai. Ce test ne certifie ni la couverture du stock, ni tous les cas de suppression, ni l'exhaustivité nationale du différentiel.

L'image Linux/amd64 a également été vérifiée hors réseau : décimaux exacts, tombstone, schéma et réutilisation d'une page committée. Les tests unitaires bloquent le réseau.

## Déploiement et activation : deux étapes distinctes

Le profil dev active `rne_refresh_parquet=true`. Deux autorisations conditionnelles permettent au seul compte `rne-refresh` de créer/lire sous `bronze/sandbox/leo/inpi/rne/`, sans suppression ni accès public. Les droits raw/control existants sont conservés.

L'image des deux jobs de collecte partage le digest :

```text
europe-west9-docker.pkg.dev/industrial-intelligence-dev/rne/ingestion@sha256:341b99394fbf67cc5f4b19df6b7a98ba6bf054e60ead56e450b9d91066049632
```

`rne-bootstrap` conserve `--archive-only` ; son image a changé mais il n'a pas été relancé pour télécharger le stock. `rne-materialize` conserve son image précédente. Le smoke test Cloud Run `rne-refresh-m5js2` a exécuté **`refresh --help` uniquement** ; il ne prouve pas une collecte API par le compte de service cloud. Les tests API/GCS précédents utilisaient le poste local et ADC.

**Voie initiale par cutoff vérifié :** elle exige une borne de reprise justifiée pour **notre archive précise**, pas une date déduite du nom `20260304`, du MDTM FTP `20261019090647`, de la fin de conversion ou du maximum des timestamps des lignes. Les pages officielles et la documentation API consultées n'ont pas fourni de garantie reliant ces métadonnées à un cutoff de ce stock.

À obtenir auprès de l'INPI ou de la documentation de livraison :

> Pour le fichier `stock_RNE_formalites_NIVEAU1_20260304_1400.zip` (MDTM `20261019090647`, SHA-256 `aebbad44755901448b9d5785cca16524dcae20852df60899f43297b205116557`), quelle borne `from` garantit un rattrapage sans lacune via `/companies/diff` ? Quelle disponibilité historique et quel traitement des retraits sont garantis ?

Une [voie alternative complète par reconstruction API](inpi-reconciliation.md) est maintenant implémentée côté collecte, sans supposer cette borne, mais pas encore initialisée/lancée nationalement ni consolidée en Silver. Elle peut remplacer l'attente de cutoff, pas supprimer le coût et les validations d'une reconstruction.

Pour la voie par cutoff vérifié :

1. Documenter la réponse et fixer la borne UTC explicite ; vérifier le reçu d'archive utilisé lors de l'initialisation pour ne pas retélécharger un nouveau stock par inadvertance.
2. Initialiser le checkpoint depuis cette base et tester un premier lot borné, puis terminer le rattrapage avec budgets/coûts approuvés.
3. Construire/vérifier la consolidation Silver par SIREN (versions, égalités de dates conflictuelles, suppressions, non-diffusion).
4. Activer le Scheduler par un plan relu et suivre la fin des exécutions, pas seulement les réponses du Scheduler.

OpenTofu refuse l'activation si la borne est vide, si `archive-only` est encore configuré ou si le Parquet différentiel n'est pas activé. Ce garde-fou ne certifie pas la borne choisie et ne remplace pas les vérifications précédentes. Aucun faux checkpoint n'a été créé pour contourner le blocage.
