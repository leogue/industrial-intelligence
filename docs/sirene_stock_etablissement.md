# Documentation Technique : `stock-stocketablissement-parquet.parquet`

Ce document détaille la structure, la volumétrie, les métadonnées et l'analyse spatiale et opérationnelle du fichier Parquet **StockEtablissement** issu de la base SIRENE v3 (INSEE / data.gouv.fr).

---

## 1. Identité et Métadonnées du Fichier

| Propriété | Valeur |
|---|---|
| **Fichier source** | `stock-stocketablissement-parquet.parquet` |
| **Origine** | INSEE via [data.gouv.fr — Base SIRENE](https://www.data.gouv.fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret) |
| **Générateur Parquet** | DuckDB (`duckdb_schema`) |
| **Taille disque (compressé)** | **2,11 Go** (2 210 114 710 octets) |
| **Nombre total de lignes** | **44 064 115** |
| **Nombre de colonnes** | **52** |
| **Clé primaire canonique** | `siret` (VARCHAR, 14 caractères = `siren` [9] + `nic` [5]) |
| **Clé étrangère (parent)** | `siren` (VARCHAR, 9 caractères -> `StockUniteLegale`) |
| **Unicité de la clé primaire** | **100% garantie** (0 doublon détecté sur 44,06M lignes) |

---

## 2. Rôle Métier dans le Projet *Industrial Intelligence*

L'Établissement représente l'**unité géographique et opérationnelle de production** (site industriel, usine, laboratoire, atelier, entrepôt, agence commerciale ou siège social). C'est le noeud d'ancrage territorial (`Establishment`) :

- **Cartographie de la souveraineté et du tissu industriel** : permet de localiser avec précision les usines et les chaînes logistiques sur le territoire français.
- **Distinction Siège vs Sites de production** : le flag `etablissementSiege` sépare les centres décisionnels des sites opérationnels.
- **Rapprochement géographique multi-échelles** : le code commune INSEE (`codeCommuneEtablissement`) permet l'agrégation par intercommunalité (EPCI), département, région et zones d'emploi.
- **Géolocalisation SIG** : coordonnées projetées en Lambert 93 (`EPSG:2154`) disponibles directement pour 75,9% des établissements.

---

## 3. Dictionnaire Complet des Données (52 colonnes)

### A. Identification & Statuts Opérationnels
| Colonne | Type Parquet | % Nulls | Description & Rôle |
|---|---|---|---|
| `siret` | `VARCHAR` | 0.0% | **Clé Primaire** — Identifiant unique d'établissement (14 caractères). |
| `siren` | `VARCHAR` | 0.0% | **Clé Étrangère** — SIREN de l'unité légale de rattachement (9 car.). |
| `nic` | `VARCHAR` | 0.0% | Numéro Interne de Classement (5 car., ex: `00014`). |
| `statutDiffusionEtablissement` | `VARCHAR` | 0.0% | Statut de diffusion : `O` (diffusable) ou `P` (protégé). |
| `dateCreationEtablissement` | `DATE` | 20.7% | Date officielle d'ouverture de l'établissement. |
| `dateDernierTraitementEtablissement`| `TIMESTAMP` | 0.0% | Horodatage de dernière synchronisation SIRENE. |
| `etablissementSiege` | `BOOLEAN` | 0.0% | **Flag Siège Social** : `True` si cet établissement est le siège de l'entreprise. |
| `nombrePeriodesEtablissement` | `BIGINT` | 0.0% | Nombre de périodes d'historique associées à l'établissement. |
| `dateDebut` | `DATE` | 2.8% | Date d'effet de la situation actuelle de l'établissement. |
| `etatAdministratifEtablissement` | `VARCHAR` | 0.0% | État administratif : `A` (Actif) ou `F` (Fermé). |

### B. Activité, Taille & Dénominations Commerciales
| Colonne | Type Parquet | % Nulls | Description & Rôle |
|---|---|---|---|
| `activitePrincipaleEtablissement` | `VARCHAR` | 0.2% | **Code NAF/APE** propre à l'activité exercée sur ce site précis. |
| `nomenclatureActivitePrincipaleEtablissement` | `VARCHAR` | 0.2% | Nomenclature de l'activité (généralement `NAFRev2`). |
| `activitePrincipaleNAF25Etablissement` | `VARCHAR` | 32.2% | Projection sur la **nomenclature NAF 2025**. |
| `activitePrincipaleRegistreMetiersEtablissement` | `VARCHAR` | 98.5% | Code Registre des Métiers (pour les artisans). |
| `trancheEffectifsEtablissement` | `VARCHAR` | 0.0% | Code tranche effectif salarié du site (`NN`, `01`, `02`, `11`...). |
| `anneeEffectifsEtablissement` | `BIGINT` | 75.4% | Année de référence des effectifs déclarés (ex: `2023`). |
| `caractereEmployeurEtablissement` | `VARCHAR` | 0.0% | Établissement employeur : `O` (Oui) ou `N` (Non). |
| `enseigne1Etablissement` | `VARCHAR` | 97.9% | Nom de l'enseigne commerciale principale. |
| `enseigne2Etablissement` | `VARCHAR` | 99.6% | Deuxième enseigne commerciale éventuelle. |
| `enseigne3Etablissement` | `VARCHAR` | 99.6% | Troisième enseigne commerciale éventuelle. |
| `denominationUsuelleEtablissement` | `VARCHAR` | 99.0% | Nom d'usage de l'établissement. |

### C. Adresse & Localisation Géographique (Adresse Principale)
| Colonne | Type Parquet | % Nulls | Description & Rôle |
|---|---|---|---|
| `codeCommuneEtablissement` | `VARCHAR` | 0.0% | **Code officiel INSEE de la commune** (5 car., ex: `13042`). Complétude réelle: **99,1%**. |
| `codePostalEtablissement` | `VARCHAR` | 0.0% | Code postal d'acheminement (5 car., ex: `28400`). Complétude réelle: **99,3%**. |
| `libelleCommuneEtablissement` | `VARCHAR` | 0.0% | Nom de la commune française. |
| `numeroVoieEtablissement` | `VARCHAR` | 21.1% | Numéro dans la voie (ex: `1`, `25`). |
| `indiceRepetitionEtablissement` | `VARCHAR` | 97.1% | Indice de répétition (ex: `B` pour Bis, `T` pour Ter). |
| `typeVoieEtablissement` | `VARCHAR` | 7.9% | Type de voie (ex: `RUE`, `AVENUE`, `BOULEVARD`, `PLACE`). |
| `libelleVoieEtablissement` | `VARCHAR` | 3.7% | Nom complet de la voie. |
| `complementAdresseEtablissement` | `VARCHAR` | 75.0% | Complément (bâtiment, zone industrielle, boîte postale). |
| `dernierNumeroVoieEtablissement` | `VARCHAR` | 99.5% | Dernier numéro en cas d'intervalle. |
| `indiceRepetitionDernierNumeroVoieEtablissement` | `VARCHAR` | 99.6% | Indice de répétition du dernier numéro. |
| `distributionSpecialeEtablissement` | `VARCHAR` | 99.6% | Distribution spéciale (ex: boîte postale, armée). |
| `codeCedexEtablissement` | `VARCHAR` | 99.6% | Code Cedex d'entreprise. |
| `libelleCedexEtablissement` | `VARCHAR` | 99.6% | Nom du bureau distributeur Cedex. |
| `codePaysEtrangerEtablissement` | `VARCHAR` | 100.0% | Code pays étranger pour les succursales hors France. |
| `libellePaysEtrangerEtablissement` | `VARCHAR` | 100.0% | Nom du pays étranger. |
| `identifiantAdresseEtablissement` | `VARCHAR` | 18.3% | **Identifiant BAN** (Base Adresse Nationale - ex: `852560033_B`). |
| `coordonneeLambertAbscisseEtablissement` | `VARCHAR` | 21.3% | **Coordonnée X en Lambert 93** (EPSG:2154). Renseigné pour 75,9% du total. |
| `coordonneeLambertOrdonneeEtablissement` | `VARCHAR` | 21.3% | **Coordonnée Y en Lambert 93** (EPSG:2154). |

### D. Champs d'Adresse Secondaire (Adresse 2)
Les 12 colonnes d'adresse secondaire (`complementAdresse2Etablissement`, `numeroVoie2Etablissement`, `typeVoie2Etablissement`, `libelleVoie2Etablissement`, `codePostal2Etablissement`, etc.) ont un taux de vacuité supérieur à **99,6%** et peuvent être exclues de la modélisation analytique courante.

---

## 4. Statistiques Opérationnelles Clés

### A. Sièges Sociaux vs Sites Secondaires
- **Sièges (`etablissementSiege = True`)** : **29 998 675 (68,08%)**
- **Sites secondaires (`etablissementSiege = False`)** : **14 065 440 (31,92%)**
> Dans l'industrie lourde et manufacturière, les grandes entreprises (GE) et ETI possèdent des dizaines voire des centaines d'établissements secondaires (usines, entrepôts, agences régionales).

### B. Précision Géographique & Rapprochement SIG
- **Présence du code commune INSEE** : **43 689 314 établissements (99,1%)**
- **Présence du code postal** : **43 771 167 établissements (99,3%)**
- **Géolocalisation Lambert 93 exacte** : **33 454 230 établissements (75,9%)**
- **Lien avec la Base Adresse Nationale (BAN)** : **35 994 000+ établissements (~81,7%)**

---

## 5. Recommandations de Traitement Silver & Gold

1. **Typage strict des clés** :
   - `siret` : `VARCHAR(14)` (`LPAD(siret, 14, '0')`).
   - `siren` : `VARCHAR(9)` (`LPAD(siren, 9, '0')`).
   - `nic` : `VARCHAR(5)` (`LPAD(nic, 5, '0')`).
2. **Standardisation Géographique vers BigQuery `GEOGRAPHY`** :
   - Les coordonnées `coordonneeLambertAbscisseEtablissement` (X) et `coordonneeLambertOrdonneeEtablissement` (Y) sont exprimées en mètres dans le système de projection national français **Lambert 93 (EPSG:2154)**.
   - Lors de l'ETL Silver -> Gold, convertir ces coordonnées vers le système mondial standard **WGS84 (EPSG:4326)** (Latitude / Longitude) pour exploiter le type natif `GEOGRAPHY` de BigQuery (`ST_GEOGPOINT(longitude, latitude)`).
3. **Partitionnement & Clustering BigQuery** :
   - Partitionner `dim_establishment` par département (extrait des 2 premiers caractères du `codeCommuneEtablissement`).
   - Clusteriser par `activitePrincipaleEtablissement` (Code NAF) et `siren`.
