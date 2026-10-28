# RNE : du stock ZIP aux entreprises en Parquet

## 1. Résultat et périmètre

`rne-ingest materialize` lit un stock **déjà archivé dans GCS**, décompresse ses fichiers JSON progressivement et écrit un snapshot Bronze Parquet. Aucun accès FTP/API INPI ni secret INPI n'est nécessaire. L'archive originale et son manifeste restent intacts.

**Essai initial du 22 octobre 2026 :** `stock_000001.json` converti en **5 000 lignes / 5 000 SIREN distincts**, **0 rejet**, Parquet de **1 672 324 octets**, puis relu et contrôlé depuis GCS. À l'issue de cet essai, le snapshot était **partiel : 1 fichier interne sur 5 470**.

**La conversion nationale est terminée avec succès** (`rne-materialize-vdrpj`, du 22 octobre à 20:10:41 UTC au 23 octobre à 03:23:55 UTC, **7 h 13 min 14 s**). Le manifeste global est publié : **5 470 / 5 470 membres**, **27 348 472 lignes valides**, **1 rejet** et **822 répétitions locales de SIREN**. Le statut qualité est `requires_review` ; l'unicité nationale reste non vérifiée. Le snapshot est techniquement complet, pas un état courant Silver certifié. Voir le [journal](../archives/deploiement-rne-dev.md) et la section 6.

Le catalogue ZIP indique **177 360 918 612 octets décompressés** pour le stock complet et un fichier interne maximal de **85 269 573 octets**. Ces nombres proviennent du répertoire central, pas d'un traitement intégral des lignes.

## 2. Organisation et contrats

```text
raw/{prefix}/stocks/.../stock.zip                    # original inchangé
control/{prefix}/stocks/{revision}.json              # reçu d'archive validé

bronze/{prefix}/companies/stock/{nom_du_zip}/        # ex. stock_RNE_formalites_NIVEAU1_20260304_1400
  part-000001.parquet
  ...
  _rejects/part-000001.jsonl.gz
  _manifests/part-000001.json                       # commit du fichier interne
  _manifest.json                                    # seulement après TOUT le stock

control/{prefix}/materialize/{sha256_archive}/
  checkpoint.json                                   # progression de conversion
```

Le sha256 de l'archive et la version de schéma ne sont plus dans le chemin : ils restent dans les manifestes et dans les colonnes de provenance. Le préfixe `_` fait ignorer manifestes et rejets par les lecteurs Parquet (DuckDB, Polars, pyarrow).

Le checkpoint du différentiel `control/{prefix}/checkpoint.json` n'est **jamais** initialisé/modifié par cette commande. La date de couverture du stock reste à confirmer avant le rattrapage API. Ni les dates min/max observées dans un premier fichier, ni son nom ne constituent un watermark national garanti.

### Schéma fixe v1 — 20 colonnes

| Colonnes | Type / sens |
|---|---|
| `siren` | Chaîne obligatoire de 9 chiffres, zéros initiaux préservés |
| `source_id` | Identifiant technique INPI, nullable |
| `source_updated_at` | Horodatage technique source, timestamp microsecondes UTC obligatoire |
| `type_personne`, `denomination`, `forme_juridique` | Chaînes nullables ; dénomination seulement si explicitement fournie dans l'identité entreprise |
| `code_ape`, `code_ape_naf2025` | Codes conservés comme chaînes distinctes |
| `diffusion_insee`, `diffusion_commerciale` | Respectivement chaîne et booléen nullables ; opposition commerciale distincte de la non-diffusion |
| `deleted` | Booléen nullable ; absence ne signifie pas automatiquement entreprise active |
| `company_json` | Objet entreprise complet resérialisé en JSON canonique v1 ; les champs variables ne sont pas supprimés |
| `company_sha256` | SHA-256 des octets UTF-8 de `company_json`, pas hash d'un sous-objet brut extrait à l'identique |
| `source_kind` | `stock` ici ; `diff` prévu par le normaliseur commun pour une future matérialisation API |
| `source_sha256`, `source_uri`, `source_generation` | Provenance vers l'archive brute et sa génération GCS, génération gardée en chaîne |
| `source_retrieved_at` | Date UTC de récupération de l'original, pas date de conversion |
| `source_member`, `source_row_number` | Nom du fichier JSON interne et position 1-indexée dans son tableau |

Les blocs RNE personne morale/physique/exploitation étant variables, on n'infère pas 5 470 schémas Parquet incompatibles. Les colonnes typées servent aux filtres/jointures ; `company_json` conserve les autres informations pour les transformations Silver ultérieures. Le ZIP conserve les octets originaux, y compris leur sérialisation.

`ijson` décode les nombres décimaux en `Decimal` et `simplejson` les resérialise sans conversion en flottant. Les dates originales restent également dans `company_json`, en plus du timestamp technique normalisé. Le même schéma est désormais utilisé pour les pages API par `refresh --with-parquet` ; voir [l'ancienne qualification du quotidien](../archives/inpi-quotidien.md). Cette collecte append-only ne consolide pas automatiquement le stock en état courant.

## 3. Validation, rejets et intégrité

- Archive choisie par révision explicite ; reçu et manifeste raw identiques, taille/génération GCS vérifiées ; lectures par plages **épinglées à cette génération**.
- Membres ZIP attendus : noms uniques `stock_NNNNNN.json`, non chiffrés ; aucune extraction dans un chemin du système de fichiers.
- Racine JSON obligatoirement un tableau. Lecture complète de chaque membre : JSON tronqué/invalide ou CRC ZIP erroné = échec de ce membre, pas de manifeste ni d'avancement.
- SIREN obligatoire et cohérent avec `formality.siren`, date technique avec fuseau, types des colonnes optionnelles contrôlés. Champs absents, `false`, `null` et chaîne vide ne sont pas confondus.
- Une ligne invalide est tracée dans un rejet gzip avec source, fichier, position et motif sûr. Les données personnelles ne sont pas copiées dans les logs/rejets ; elles restent localisables dans le raw privé.
- Les doublons de SIREN **dans un membre** sont comptés et conservés, sans choix arbitraire de version. L'unicité globale entre membres n'est pas encore vérifiée ; le manifeste le précise.
- Parquet compressé Zstandard, schéma identique pour chaque part ; hash des octets produits, taille et génération conservés dans le manifeste de membre.
- Un snapshot techniquement complet peut avoir `quality_status=requires_review` (rejets, doublons locaux ou zéro ligne). `member_checks_passed` ne prouve ni l'unicité globale ni la qualité juridique.

Les sorties restent privées et portent les indicateurs de diffusion. Leur existence n'autorise pas à republier des personnes physiques ou entreprises non diffusibles. Pas d'inférence d'actionnariat depuis des représentants.

## 4. Reprise et lecture correcte des sorties

Le manifeste d'un membre est écrit **après** fermeture/validation du Parquet et du fichier de rejets. Le checkpoint avance seulement ensuite, avec précondition de génération. Si cette dernière écriture échoue, une relance valide et réutilise le manifeste existant au lieu de réécrire la part.

Chaque part a un nom fixe et le job ne peut que créer des objets dans `bronze/` (pas de remplacement). Si un processus meurt entre l'upload d'une part et l'écriture de son manifeste, la relance échoue avec `Output object already exists` : vérifier qu'aucune exécution n'est active, supprimer à la main `part-NNNNNN.parquet` et `_rejects/part-NNNNNN.jsonl.gz` du membre sans manifeste, puis relancer. Cette fenêtre est très courte et ne s'est pas produite sur le stock national.

Un snapshot est complet **seulement si `_manifest.json` existe**. Il se lit alors directement, par exemple avec DuckDB : `read_parquet('gs://…/companies/stock/{nom_du_zip}/*.parquet')`. Avant publication du manifeste global, toutes les parts acquittées et leurs totaux sont revérifiés.

Une relance avec `--max-members 1` traite le **prochain** membre non acquitté. Une relance après complétion ne réécrit pas les données. Un changement de schéma/règles de normalisation incompatible nécessite une nouvelle version de schéma, pas l'écrasement d'un snapshot existant.

Le verrou est partagé avec l'ingestion dans ce namespace. Un arrêt brutal peut laisser un verrou : vérifier les jobs actifs avant toute récupération manuelle autorisée. Un budget temporel souple permet normalement une sortie propre entre deux membres.

## 5. Commandes et limites

```sh
uv sync --locked
uv run rne-ingest materialize --help

# Essai borné sur le stock archivé (préfixe par défaut inpi/rne). ADC, pas de .env nécessaire.
# Une nouvelle exécution passe au membre suivant si le premier est déjà acquitté.
uv run rne-ingest --bucket industrial-intelligence-dev-data materialize \
  --stock-revision b47860c7c6a645bc1d54e878d2c4d2b5ec67b2d4530da9b1bd5f2cd2c8c9fbde \
  --max-members 1
```

Valeurs par défaut : un nouveau membre, **128 MiB** de JSON décompressé par exécution et par membre, lots de **500 lignes ou environ 8 MiB de JSON**, budget souple de **1 800 s**. Une entreprise entière est matérialisée avant sa mise en lot, avec limite de représentation normalisée de **16 MiB** ; ce n'est pas une garantie de mémoire constante face à un objet source pathologique. L'essai réel a utilisé environ **131 MiB de RSS maximale sur Mac**, pas une mesure Cloud Run.

`--all-members` supprime seulement la limite de nombre de fichiers. Les limites d'octets et de durée restent actives ; elles doivent être ajustées explicitement après budget/qualification du volume national. Une limite temporelle rend un résultat `partial` propre ; la limite d'octets lève une pause (code 2), en conservant les membres terminés. Les erreurs fatales ne font pas avancer le membre en cours.

Aucun transfert FTP supplémentaire : seul le ZIP GCS existant est lu par plages. L'essai initial a porté sur 21,6 Mo de JSON / 1,43 Mo compressés, pas sur les 177 Go décompressés. Ne pas extrapoler automatiquement temps, compression et qualité de ce premier fichier à tous les autres.

## 6. Cloud Run — déployé et traitement national terminé

`infra/opentofu/rne_materialize.tf` définit le job manuel opt-in `rne-materialize`, désormais déployé en dev :

- `enable_rne_materialization=false` par défaut ;
- image dédiée par digest dans `rne_materialize_image`, contenant la commande `materialize` ; même recette Docker que l'ingestion, rollout indépendant des deux jobs existants ;
- service account `rne-materialize`, **aucun secret INPI** ;
- lecture raw, création/lecture bronze et manipulation control, conditionnelles au namespace ; aucun droit de suppression/remplacement du raw/bronze ;
- une tâche, **1 vCPU / 2 GiB**, timeout de 6 h, aucun retry automatique, aucun Scheduler ;
- arguments initiaux limités à un membre ; révision de stock explicite dans `rne_stock_revision`.

L'image `industrial-intelligence-rne:materialize` a été construite/testée en Linux/amd64 sous émulation, puis publiée sous :

```text
europe-west9-docker.pkg.dev/industrial-intelligence-dev/rne/ingestion@sha256:df16dac1a83946c1eb09a01fc18271c19d6839d19d2ee88e350a704d4a2d7fed
```

Après approbation explicite, le plan a été **appliqué : 6 créations, 0 modification, 0 suppression** (job, compte de service, quatre autorisations GCS). Le profil `envs/rne-dev.tfvars` active désormais ce worker avec l'image et la révision ci-dessus. Les deux jobs d'ingestion existants et leur Scheduler en pause restent inchangés. Un plan avec les deux fichiers de variables a confirmé `No changes` après le lancement.

### Lancement national approuvé

Une seule tâche, **1 vCPU / 2 GiB**, aucun retry automatique. Les premières mesures Cloud Monitoring donnent environ **60–65 % de CPU** et **90–94 MiB de mémoire** sur les minutes observées ; elles ne justifient pas de doubler les ressources à ce stade et ne garantissent pas le pic mémoire de tout le stock. Le code reste séquentiel, sans workers concurrents ni changement de verrou/checkpoint.

Commande exécutée le 22 octobre 2026 ; **ne pas relancer pendant qu'une exécution détient le verrou** :

```sh
gcloud run jobs execute rne-materialize \
  --project=industrial-intelligence-dev --region=europe-west9 \
  --tasks=1 --task-timeout=86400s \
  --args=materialize,--stock-revision,b47860c7c6a645bc1d54e878d2c4d2b5ec67b2d4530da9b1bd5f2cd2c8c9fbde,--all-members,--max-input-bytes,200000000000,--max-seconds,82800 \
  --async
```

Le budget d'entrée de **200 Go décompressés** couvre les **177,36 Go** connus ; la limite par membre de 128 MiB reste inchangée. Le budget souple de **23 h** et le timeout d'exécution de **24 h** sont des overrides de cette exécution, pas des modifications du défaut prudent du job (un membre, timeout 6 h). Ce sont des limites de durée/volume, **pas un plafond de facture**. L'ordre de grandeur annoncé avant lancement était plusieurs heures et quelques dollars de calcul hors quotas gratuits, plus opérations/stockage ; le montant réel n'a pas été vérifié.

À **20:15:57 UTC**, le checkpoint indiquait **66 / 5 470 membres**, **330 001 lignes valides**, **0 rejet**, **1 répétition de SIREN dans un membre**. Les enregistrements correspondants sont conservés ; ce compteur ne mesure pas l'unicité nationale. Le premier Parquet écrit par Cloud Run (`part-000002`) a été relu intégralement : SHA-256 binaire, schéma, 5 000 identifiants de neuf chiffres, empreintes JSON et provenance vérifiés.

```sh
# État géré par Cloud Run : le terminal local peut être fermé.
gcloud run jobs executions describe rne-materialize-vdrpj \
  --project=industrial-intelligence-dev --region=europe-west9
```

Un résultat `partial` à la limite temporelle exige une relance explicite après vérification de la fin du worker/verrou. Aucun mécanisme n'a été ajouté pour relancer indéfiniment ou contourner le verrou. Le checkpoint différentiel reste absent et le Scheduler reste `PAUSED`.

## 7. Étapes restantes

1. Examiner l'unique rejet, les 822 répétitions locales et l'unicité globale par SIREN, sans arbitrer des versions sans règle métier.
2. Vérifier le coût facturé ; la taille observée est de l'ordre de 16,20 Go de Bronze pour 177,36 Go de JSON décompressés, en plus du ZIP intact de 15,14 Go.
3. Confirmer une borne de couverture stock → flux puis initialiser/rattraper le différentiel brut + Parquet déjà disponible ; voir [le guide d'ingestion actuel](inpi-ingestion.md).
4. Construire l'état courant Silver et les tables métiers avant de présenter une base analytique automatiquement à jour.

Le raw national est conservé et **la matérialisation nationale est techniquement complète, avec manifeste global publié et qualité à examiner**. Aucun Spark, Iceberg, GraphDB ni base locale n'est ajouté pour cette étape.
