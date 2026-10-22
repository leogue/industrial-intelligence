# Documentation Technique : `stock-stocketablissementhistorique-parquet.parquet`

Ce document détaille la structure, la volumétrie, les métadonnées et la modélisation temporelle (SCD Type 2) du fichier Parquet **StockEtablissementHistorique** issu de la base SIRENE v3 (INSEE / data.gouv.fr).

---

## 1. Identité et Métadonnées du Fichier

| Propriété | Valeur |
|---|---|
| **Fichier source** | `stock-stocketablissementhistorique-parquet.parquet` |
| **Origine** | INSEE via [data.gouv.fr — Base SIRENE](https://www.data.gouv.fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret) |
| **Générateur Parquet** | DuckDB (`duckdb_schema`) |
| **Taille disque (compressé)** | **833,03 Mo** (873 497 400 octets) |
| **Nombre total de lignes** | **96 228 441** |
| **Nombre de colonnes** | **18** |
| **Type de modélisation** | **Slowly Changing Dimension Type 2 (SCD2)** |
| **Clé primaire composite** | (`siret`, `dateDebut`) |
| **Clés étrangères** | `siret` (14 car. -> `StockEtablissement`), `siren` (9 car. -> `StockUniteLegale`) |

---

## 2. Rôle Métier dans le Projet *Industrial Intelligence*

Dans la plateforme *Industrial Intelligence*, ce fichier permet l'**analyse temporelle et dynamique du tissu industriel français** :

- **Traçabilité des fermetures et créations d'usines** : identification précise de la date de fermeture (`etatAdministratifEtablissement = 'F'`) ou de réactivation d'un site.
- **Détection des reconversions industrielles (Pivots sectoriels)** : suivi des modifications de code NAF au cours de la vie de l'établissement (ex: passage d'une activité textile à la fabrication de masques, ou métallurgie vers aéronautique).
- **Évolution de l'emploi industriel** : suivi du passage de statut non-employeur à employeur (`caractereEmployeurEtablissement`).
- **Calcul des durées de vie des établissements** : `DATEDIFF(COALESCE(dateFin, CURRENT_DATE), dateDebut)` pour calculer le taux de survie des entreprises par secteur.

---

## 3. Dictionnaire Complet des Données (18 colonnes)

| Colonne | Type Parquet | % Nulls | Description Métier & Rôle Temporel | Exemples de Valeurs |
|---|---|---|---|---|
| `siren` | `VARCHAR` | 0.0% | SIREN de l'unité légale mère (9 car.). | `213104052`, `176604312` |
| `nic` | `VARCHAR` | 0.0% | Numéro interne de classement (5 car.). | `00017`, `00010` |
| `siret` | `VARCHAR` | 0.0% | **Clé d'établissement** (14 car.). Clé de liaison principale. | `17660431200031` |
| `dateDebut` | `DATE` | 2.8% | **Date de début de validité de la tranche d'état** (Borne inférieure SCD2). | `1995-12-25`, `2008-01-01` |
| `dateFin` | `DATE` | 49.4% | **Date de fin de validité de la tranche d'état** (`NULL` si tranche courante/active). | `2026-07-30`, `NULL` |
| `etatAdministratifEtablissement` | `VARCHAR` | 0.6% | État administratif durant cette période : `A` (Actif) ou `F` (Fermé). | `A`, `F` |
| `changementEtatAdministratifEtablissement` | `BOOLEAN` | 0.0% | **Flag de transition** : `True` si cette tranche matérialise une ouverture/fermeture. | `True`, `False` |
| `enseigne1Etablissement` | `VARCHAR` | 94.0% | Première enseigne de l'établissement sur cette période. | `SYND. COPR. 3 RUE DE LYON` |
| `enseigne2Etablissement` | `VARCHAR` | 98.7% | Deuxième enseigne sur cette période. | `STEPHANE MACQUAIRE` |
| `enseigne3Etablissement` | `VARCHAR` | 98.8% | Troisième enseigne sur cette période. | `MENEO` |
| `changementEnseigneEtablissement` | `BOOLEAN` | 0.0% | **Flag de transition** : `True` si l'enseigne a changé lors de cette période. | `True`, `False` |
| `denominationUsuelleEtablissement` | `VARCHAR` | 97.9% | Dénomination usuelle sur la période. | `PASSIONNEMENT GLAMOUR` |
| `changementDenominationUsuelleEtablissement`| `BOOLEAN` | 0.0% | **Flag de transition** : `True` si le nom d'usage a changé. | `True`, `False` |
| `activitePrincipaleEtablissement` | `VARCHAR` | 12.2% | **Code NAF/APE** exercé par l'établissement durant cette période. | `70.10Z`, `93.29Z`, `25.11Z` |
| `nomenclatureActivitePrincipaleEtablissement` | `VARCHAR` | 12.2% | Version de nomenclature (ex: `NAFRev2`, `NAP`, `NAF1993`). | `NAFRev2`, `NAP` |
| `changementActivitePrincipaleEtablissement` | `BOOLEAN` | 0.0% | **Flag de transition** : `True` si l'activité NAF a changé (reconception/pivot). | `True`, `False` |
| `caractereEmployeurEtablissement` | `VARCHAR` | 11.8% | Présence de salariés sur la période : `O` (Oui), `N` (Non). | `O`, `N` |
| `changementCaractereEmployeurEtablissement` | `BOOLEAN` | 0.0% | **Flag de transition** : `True` si le statut d'employeur a évolué. | `True`, `False` |

---

## 4. Analyse Statistique des Évolutions Temporelles

Sur les **96 228 441 lignes d'historique**, l'analyse des flags de changement révèle la dynamique réelle du tissu économique français :

| Type de changement historique | Nombre de tranches impactées | % du volume total historique | Utilité pour Industrial Intelligence |
|---|---|---|---|
| **Changement d'État Administratif** | **28 670 756** | **29,8%** | Détection exacte des vagues de fermetures industrielles et des réactivations de friches. |
| **Changement d'Activité NAF** | **20 838 694** | **21,7%** | Cartographie des mutations sectorielles (usines changeant de spécialité technique). |
| **Changement de Statut Employeur** | **10 002 143** | **10,4%** | Suivi du franchissement de seuils de recrutement et des destructions d'emplois. |
| **Changement d'Enseigne** | **606 201** | **0,6%** | Suivi des rachats, cessions de fonds de commerce ou rebrandings. |
| **Changement de Dénomination Usuelle**| **570 984** | **0,6%** | Suivi de l'identité de marque opérationnelle. |

---

## 5. Recommandations de Modélisation Temporelle & BigQuery

1. **Reconstitution de la situation courante** :
   - La tranche courante d'un établissement possède toujours `dateFin IS NULL`.
   - On retrouve exactement les 44 064 115 états actuels de `StockEtablissement` en filtrant :
     ```sql
     SELECT * FROM stock_etablissement_historique WHERE dateFin IS NULL;
     ```
2. **Reconstitution d'un état à une date passée T (Point-in-Time Query)** :
   ```sql
   SELECT siret, activitePrincipaleEtablissement, etatAdministratifEtablissement
   FROM stock_etablissement_historique
   WHERE dateDebut <= DATE '2020-01-01'
     AND (dateFin > DATE '2020-01-01' OR dateFin IS NULL);
   ```
3. **Partitionnement & Clustering BigQuery** :
   - En raison de ses **96 millions de lignes**, cette table doit être impérativement optimisée pour éviter des scans intégraux coûteux.
   - **Partitionnement** : par mois ou année sur la colonne `dateDebut`.
   - **Clustering** : sur `siret` et `siren` (permet de scanner uniquement les micro-partitions d'une entreprise donnée en quelques millisecondes).
