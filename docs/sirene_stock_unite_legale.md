# Documentation Technique : `stock-stockunitelegale-parquet.parquet`

Ce document détaille la structure, la volumétrie, les métadonnées et l'analyse de qualité du fichier Parquet **StockUniteLegale** issu de la base SIRENE v3 (INSEE / data.gouv.fr).

---

## 1. Identité et Métadonnées du Fichier

| Propriété | Valeur |
|---|---|
| **Fichier source** | `stock-stockunitelegale-parquet.parquet` |
| **Origine** | INSEE via [data.gouv.fr — Base SIRENE](https://www.data.gouv.fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret) |
| **Générateur Parquet** | DuckDB (`duckdb_schema`) |
| **Taille disque (compressé)** | **675,73 Mo** (708 557 254 octets) |
| **Nombre total de lignes** | **30 020 346** |
| **Nombre de colonnes** | **35** |
| **Clé primaire canonique** | `siren` (VARCHAR, 9 caractères avec zéros initiaux) |
| **Unicité de la clé primaire** | **100% garantie** (0 doublon détecté sur 30,02M lignes) |

---

## 2. Rôle Métier dans le Projet *Industrial Intelligence*

Dans l'architecture *Industrial Intelligence*, l'Unité Légale représente la **personne morale ou physique juridique** (société anonyme, SAS, SARL, micro-entrepreneur, association employeuse). C'est le **noeud central** (`Company`) du modèle analytique et du graphe d'entreprises :

- **Clé de réconciliation universelle** : le code `siren` (9 chiffres) permet la jointure directe avec les bilans financiers (INPI RNE), les actes juridiques, les annonces BODACC et les marchés publics (BOAMP/DECP).
- **Point de rattachement des établissements** : une unité légale possède un ou plusieurs établissements (SIRET).
- **Lien avec le siège social** : le champ `nicSiegeUniteLegale` (5 chiffres) permet de reconstituer instantanément le SIRET du siège social (`siren` + `nicSiegeUniteLegale`).

---

## 3. Dictionnaire Complet des Données (35 colonnes)

| Colonne | Type Parquet | % Valeurs Nulles (Échantillon) | Description & Utilité Métier | Exemples de Valeurs |
|---|---|---|---|---|
| `siren` | `VARCHAR` | 0.0% | **Clé Primaire** — Numéro unique d'identification d'entreprise (9 car.). | `103576534`, `005420021` |
| `statutDiffusionUniteLegale` | `VARCHAR` | 0.0% | Statut de diffusion : `O` (diffusable public) ou `P` (partielle / protégé RGPD). | `O`, `P` |
| `unitePurgeeUniteLegale` | `BOOLEAN` | 77.2% | Indique si l'unité légale a été purgée de l'historique SIRENE. | `True` |
| `dateCreationUniteLegale` | `DATE` | 9.7% | Date officielle d'immatriculation / création de l'entreprise. | `2000-09-26`, `1972-05-01` |
| `sigleUniteLegale` | `VARCHAR` | 99.4% | Sigle de l'entreprise (souvent null). | `SBTP`, `SODIPA` |
| `sexeUniteLegale` | `VARCHAR` | 79.4% | Sexe du dirigeant si entrepreneur individuel (`M`, `F`, `[ND]`). | `M`, `F` |
| `prenom1UniteLegale` | `VARCHAR` | 79.4% | Premier prénom (personnes physiques uniquement). | `THIERRY`, `GEORGES` |
| `prenom2UniteLegale` | `VARCHAR` | 95.5% | Deuxième prénom. | `ALFRED`, `PIERRE` |
| `prenom3UniteLegale` | `VARCHAR` | 98.5% | Troisième prénom. | `CYRILLE`, `ANDRE` |
| `prenom4UniteLegale` | `VARCHAR` | 99.7% | Quatrième prénom. | `MARIE`, `JEAN` |
| `prenomUsuelUniteLegale` | `VARCHAR` | 79.4% | Prénom usuel du dirigeant personne physique. | `THIERRY`, `JACQUES` |
| `pseudonymeUniteLegale` | `VARCHAR` | 99.8% | Pseudonyme artistique / professionnel éventuel. | `CARON`, `COURCOL` |
| `identifiantAssociationUniteLegale` | `VARCHAR` | 100.0% | Numéro RNA pour les associations (format `W...`). | `W443005015` |
| `trancheEffectifsUniteLegale` | `VARCHAR` | 0.0% | Code tranche d'effectifs salariés (INSEE : `NN`, `01`, `02`...). | `NN`, `02`, `22` |
| `anneeEffectifsUniteLegale` | `BIGINT` | 74.4% | Année de validité de la tranche d'effectifs déclarée. | `2023` |
| `dateDernierTraitementUniteLegale` | `TIMESTAMP` | 0.0% | Horodatage technique de la dernière mise à jour dans SIRENE. | `2025-12-06 05:24:47` |
| `nombrePeriodesUniteLegale` | `BIGINT` | 0.0% | Nombre de tranches historiques d'évolution de l'entreprise. | `1`, `5`, `6` |
| `categorieEntreprise` | `VARCHAR` | 51.7% | Classification économique de taille : `PME`, `ETI`, `GE`. | `PME`, `ETI`, `GE` |
| `anneeCategorieEntreprise` | `BIGINT` | 51.7% | Année de calcul de la catégorie d'entreprise. | `2023` |
| `dateDebut` | `DATE` | 1.6% | Date d'effet de la situation courante de l'entreprise. | `2018-02-07`, `2014-12-31` |
| `etatAdministratifUniteLegale` | `VARCHAR` | 0.0% | État d'activité : `A` (Active) ou `C` (Cessée/Fermée). | `A`, `C` |
| `nomUniteLegale` | `VARCHAR` | 79.4% | Nom de naissance pour les entrepreneurs individuels. | `SCHABERG`, `HASNI` |
| `nomUsageUniteLegale` | `VARCHAR` | 96.6% | Nom d'usage éventuel pour personne physique. | `VANDAMME`, `LOUCHART` |
| `denominationUniteLegale` | `VARCHAR` | 20.4% | **Raison sociale / Nom commercial officiel des sociétés**. | `BASALPINE AUTOCARS` |
| `denominationUsuelle1UniteLegale` | `VARCHAR` | 98.7% | Première dénomination d'usage de la société. | `PHARMACIE COMTE` |
| `denominationUsuelle2UniteLegale` | `VARCHAR` | 99.8% | Deuxième dénomination d'usage. | `LE CARTONNIER DU VIN` |
| `denominationUsuelle3UniteLegale` | `VARCHAR` | 99.8% | Troisième dénomination d'usage. | `LES PRODUCTEURS DU VAUCLUSE` |
| `categorieJuridiqueUniteLegale` | `BIGINT` | 0.0% | Code forme juridique INSEE (ex: `5710`=SAS, `5499`=SARL). | `5710`, `5499`, `1000` |
| `activitePrincipaleUniteLegale` | `VARCHAR` | 0.2% | **Code NAF/APE** (Nomenclature d'Activités Française). | `62.01Z`, `46.69B`, `70.10Z` |
| `nomenclatureActivitePrincipaleUniteLegale` | `VARCHAR` | 0.2% | Version de la nomenclature d'activité. | `NAFRev2`, `NAP`, `NAF1993` |
| `nicSiegeUniteLegale` | `VARCHAR` | 0.0% | **NIC du siège social** (5 car., zéros inclus). | `00011`, `00056`, `00033` |
| `economieSocialeSolidaireUniteLegale` | `VARCHAR` | 90.5% | Appartenance à l'Économie Sociale et Solidaire (`O`/`N`). | `O`, `N` |
| `societeMissionUniteLegale` | `VARCHAR` | 100.0% | Qualité de Société à Mission issue de la loi PACTE (`O`/`N`). | `O`, `N` |
| `caractereEmployeurUniteLegale` | `VARCHAR` | 100.0% | Indicateur employeur de l'unité légale (`O`/`N`). | `O`, `N` |
| `activitePrincipaleNAF25UniteLegale` | `VARCHAR` | 27.5% | **Nouvelle nomenclature NAF 2025** projetée par l'INSEE. | `32.12Y`, `46.64J`, `70.10Y` |

---

## 4. Distributions et Analyses Statistiques Clés

### A. État d'Activité (`etatAdministratifUniteLegale`)
- **Actives (`A`)** : **16 662 023 (55,5%)** — Entreprises actuellement en exercice.
- **Cessées (`C`)** : **13 358 323 (44,5%)** — Entreprises radiées ou fermées.
> **Règle d'or ETL** : Pour l'analyse de santé industrielle en production, filtrer par défaut sur `etatAdministratifUniteLegale = 'A'` afin d'éviter d'ingérer 13,3 millions de sociétés radiées dans les tables chaudes de BigQuery.

### B. Catégorie d'Entreprise (`categorieEntreprise`)
- **GE (Grandes Entreprises)** : 43 364 (0,14%)
- **ETI (Entreprises de Taille Intermédiaire)** : 108 024 (0,36%)
- **PME (Petites et Moyennes Entreprises)** : 10 394 208 (34,62%)
- **Non renseigné / Micro-entreprises** : 19 474 750 (64,87%)

### C. Présence de la Raison Sociale
- `denominationUniteLegale` est renseignée à 79,6% (sociétés commerciales, SA, SAS, SARL, SCI).
- Lorsque `denominationUniteLegale` est `NULL`, le nom de l'entité se reconstitue via `nomUniteLegale` + `' '` + `prenomUsuelUniteLegale` (personnes physiques et entrepreneurs individuels).

---

## 5. Recommandations de Traitement Silver & Gold

1. **Typage strict & Formatage des identifiants** :
   - Forcer `siren` au format `VARCHAR(9)` avec complétion par des zéros à gauche (`LPAD(siren, 9, '0')`).
   - Forcer `nicSiegeUniteLegale` en `VARCHAR(5)` (`LPAD(nicSiegeUniteLegale, 5, '0')`).
2. **Champ calculé `company_name`** :
   ```sql
   COALESCE(denominationUniteLegale, CONCAT_WS(' ', nomUniteLegale, prenomUsuelUniteLegale), '[SANS NOM]') AS company_name
   ```
3. **Partitionnement & Clustering BigQuery** :
   - Clusteriser la table Gold `dim_company` par `activitePrincipaleUniteLegale` (Code NAF) et `categorieEntreprise`.
   - Partitionner si nécessaire par tranche d'années de création (`dateCreationUniteLegale`).
