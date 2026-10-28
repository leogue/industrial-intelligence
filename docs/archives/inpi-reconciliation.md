# Archive — ancien rejeu intégral de la base API

> **Stratégie abandonnée dans le code local simplifié.** `init-reconciliation` et les branches de rejeu depuis l'année 1 ont été retirées. Le texte et les commandes ci-dessous décrivent l'ancienne implémentation ; **ne pas les utiliser comme procédure actuelle**. Le nouveau [guide sync](inpi-sync.md) retient stock + rattrapage depuis une borne explicite (hypothèse du 1er mars 2026) + quotidien. Le déploiement et les checkpoints cloud restent sur l'ancienne version tant qu'une migration n'a pas été approuvée/exécutée.

> **Mode retenu le 24 octobre : [sync GCS + Parquet](inpi-sync.md), sans BigQuery.** Le catalogue courant est disponible ; les changements récents sont prioritaires et l'historique avance dans une réserve de 500 appels par exécution, au sein des 5 000 appels totaux. Il ne s'agit pas du rechargement national non borné décrit comme scénario de coût plus bas. Le Scheduler est activé après qualification cloud, sans déclarer la réconciliation historique déjà terminée.

## Choix durable, sans inventer le cutoff du ZIP

Plutôt que déduire une couverture métier du nom/MDTM du stock, reconstruire les **états accessibles via l'API**, conserver leur provenance, consolider par SIREN puis passer au quotidien. Le ZIP et le snapshot Bronze existants restent intacts. Une absence dans une API ne prouve pas une cessation juridique.

Ce n'est pas un rejeu garanti de tous les événements historiques : la rétention des retraits et la sémantique exacte de sélection ne sont pas certifiées. Une base courante reconstruite doit être distinguée d'un historique juridique exhaustif.

## Constat réel qui corrige le collecteur

Audit du 23 octobre 2026 :

- `/companies/diff/count` pour **2023-01-02 12:00–13:00 UTC** : 16 résultats.
- Pagination : **16 fiches distinctes, puis page vide**.
- Les 16 `company.updatedAt` sont entre **2026-07-31 et 2026-10-13**, hors fenêtre 2023.
- `formality.updated` est absent de 10 fiches ; les six autres valeurs sont aussi hors fenêtre. Il ne peut pas remplacer automatiquement le timestamp racine comme date de sélection.
- Le count pour `0001-01-01 → 2022-12-31` et celui pour `2000-01-01 → 2022-12-31` donnent tous deux **3 179** ; `0001-01-01 → 1999-12-31` donne **0**. Les grandes périodes 2023–2026 dépassent le plafond de count de 10 000.

Ces observations montrent que **le timestamp de la fiche retournée n'est pas une preuve de son appartenance à la fenêtre de sélection**. Elles ne prouvent pas que les filtres sont ignorés, ni la cause interne du comportement.

Le collecteur valide toujours le format des identifiants et des timestamps, conserve les fiches et compte `company_timestamps_outside_window`. Il ne jette plus des états courants légitimes sur ce seul critère.

Avant de clôturer une fenêtre, il exige désormais :

1. count exact disponible sous le plafond de 10 000 ; découpage sinon ;
2. aucun SIREN répété entre les pages, ni curseur manquant/répété ;
3. page vide terminale et nombre de lignes égal au count initial ;
4. second count identique après pagination ;
5. brut et Parquet/manifeste sauvegardés avant checkpoint.

Un écart bloque la progression et demande une reprise explicite de fenêtre ; pas d'exhaustivité déclarée malgré une pagination instable. Des counts égaux ne constituent pas un snapshot transactionnel côté fournisseur. La réconciliation périodique et les règles de restitution restent nécessaires.

La qualification complète de cette fenêtre historique a réussi via **5 appels API**, avec Parquet en mémoire et **aucune écriture GCS de production**. Les contrôles de counts, de doublons et de reprise sont couverts par les tests hors réseau.

## Initialisation explicite de reconstruction (opération avancée)

```sh
# À exécuter seulement après validation du budget de reconstruction.
uv run rne-ingest --bucket industrial-intelligence-dev-data \
  --prefix sandbox/leo/inpi/rne init-reconciliation \
  --stock-revision b47860c7c6a645bc1d54e878d2c4d2b5ec67b2d4530da9b1bd5f2cd2c8c9fbde
```

Cette commande vérifie le reçu GCS, la génération/taille de l'archive et le manifeste global de stock complet. Elle ne contacte pas le FTP/INPI, ne réécrit aucune donnée et refuse de remplacer un checkpoint existant.

Elle prépare un checkpoint `strategy=full_api_reconciliation`, `output_mode=raw_and_parquet`, `coverage_status=rebuild_pending`. La borne est la **première date UTC représentable (`0001-01-01`)**, acceptée dans les probes API ; **ce n'est pas un cutoff supposé du ZIP**. `stock_cutoff_verified=false` reste explicite. Une cible de réconciliation est gelée à l'initialisation et renvoyée à l'opérateur.

Lors du premier rattrapage, les fenêtres partent de l'intervalle restant jusqu'à l'horizon demandé et sont divisées par deux selon le count, plutôt que parcourir deux millénaires par créneaux de six heures. Les budgets pages/requêtes et la sauvegarde après chaque page restent applicables. Un créneau d'une seconde encore au-dessus de 10 000 bloque : aucun contournement de limite fournisseur n'est ajouté.

Un lot terminé avant la cible gelée reste `rebuild_pending`. Quand cette cible est atteinte, le collecteur marque **`awaiting_silver_reconciliation`**, pas « base prête ». Les collectes suivantes reviennent aux fenêtres quotidiennes bornées. Un ancien checkpoint de fenêtre sans les nouveaux contrôles doit être repris avec `--restart-window`, pas converti implicitement.

## Reste indispensable avant activation automatique

- Valider le budget et la disponibilité/les quotas du compte pour un chargement API national.
- Initialiser puis terminer la collecte complète avec suivi des counts, des quotas et des erreurs ; arrêter/reprendre, sans boucle de retries infinie.
- La consolidation Silver est désormais disponible comme **vue indexée logique GCS**, testée en cloud : SIREN, versions, tombstones, conflits et provenance ; elle ne copie pas tous les JSON dans un nouveau gros fichier.
- Réconcilier les identifiants du stock et ceux disponibles via l'API, en traitant les absences comme indisponibilités à examiner, pas comme des cessations inférées.
- Le catalogue courant et son pointeur atomique sont publiés ; le garde-fou vérifie désormais le marqueur GCS réel de `prepare-sync`. Le Scheduler est activé après essai cloud ; la couverture historique reste exposée comme `reconciling`.

La voie ancienne par cutoff vérifié reste disponible. Le mode `sync` utilise une préparation réelle avec catalogue Silver et checkpoints de récent/historique distincts ; le marqueur vérifié autorise la planification. Son état initial ne prétend pas certifier le cutoff du stock ni une reconstruction déjà complète. Voir le journal du 24 octobre pour les opérations réelles.

## Coût/volume avant tout lancement national

Ordre de grandeur, pas devis fournisseur : la base actuelle contient environ **27,35 millions de lignes**. À 100 fiches/page, un parcours comparable représente environ **273 500 pages**, plus counts, pages vides, authentifications et retries. Les fiches peuvent réapparaître entre fenêtres : le nombre réel peut être supérieur. L'espacement de 0,25 s implique déjà **environ 19 h d'attente cumulée minimale**, avant réseau/calcul/GCS ; durée potentiellement de plusieurs jours, selon quotas non confirmés pour cette API.

Le stock représente 177 Go de JSON : un parcours API comparable pourrait ajouter **de l'ordre de 180–250 Go de brut JSON**, plus les Parquet, manifestes et versions de contrôle conservées temporairement. Ces réponses sont actuellement stockées sans gzip. Le volume réel dépendra des répétitions et du contenu actuel des fiches.

Pour décider : prévoir une **enveloppe indicative de 20–30 € pour collecte/calcul/opérations**, et **plusieurs euros de stockage mensuel supplémentaire** ; ceci n'inclut pas un futur moteur analytique payant et n'est pas un plafond automatique. L'ancien essai FTP/stock à petit budget ne vaut pas autorisation de ce second chargement national. **Ce chargement n'a pas été lancé.**

**Mesures de coûts complémentaires :** [couts-rne-dev.md](../rne/couts-rne-dev.md) détaille les métriques disponibles, les tarifs officiels et un benchmark réel. Les gros jobs déjà réalisés ne représentent qu'environ **0,59 $ de calcul brut**, potentiellement couvert par le quota gratuit ; l'enveloppe 20–30 € n'était pas leur coût ni celui d'un rattrapage quotidien. Les écritures très nombreuses et le brut API non compressé expliquent une grande partie du scénario complet. Un rattrapage ciblé de quelques jours peut être beaucoup moins coûteux, une fois sa couverture justifiée.
