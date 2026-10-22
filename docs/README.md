# Documentation — Industrial Intelligence

Ce dossier regroupe la documentation technique, fonctionnelle et d'architecture des données du projet **Industrial Intelligence**.

---

## 📚 Référentiel des Sources de Données

### 🏢 Base SIRENE (INSEE / data.gouv.fr)

La base SIRENE constitue le référentiel pivot d'identification et de géolocalisation des entreprises et établissements français :

- [`sirene.md`](./sirene.md) : **Guide d'accès aux données, API data.gouv.fr & stratégie d'ingestion idempotente** (interrogation de l'API catalogue, extraction des hashs SHA-1 distants, validation de versions et téléchargement streaming vers GCS).
- [`sirene_stock_unite_legale.md`](./sirene_stock_unite_legale.md) : Analyse exhaustive du fichier `stock-stockunitelegale-parquet.parquet` (30 020 346 entreprises, clé primaire `siren`, 35 colonnes).
- [`sirene_stock_etablissement.md`](./sirene_stock_etablissement.md) : Analyse exhaustive du fichier `stock-stocketablissement-parquet.parquet` (44 064 115 implantations physiques, clé `siret`, coordonnées Lambert 93, codes communes INSEE, 52 colonnes).
- [`sirene_stock_etablissement_historique.md`](./sirene_stock_etablissement_historique.md) : Analyse du fichier `stock-stocketablissementhistorique-parquet.parquet` (96 228 441 tranches temporelles SCD2, suivi des fermetures, reconversions sectorielles et changements d'activité, 18 colonnes).
- [`sirene_interconnexions_et_modelisation.md`](./sirene_interconnexions_et_modelisation.md) : Architecture de jointure interne (1:N, siège 1:1, SCD2), croisements externes (INPI RNE bilans & actes, BODACC, BOAMP, COG) et modèle dimensionnel / graphe cible pour GCS et BigQuery.

---

### 🏛️ INPI RNE (Registre National des Entreprises)

- [`inpi.md`](./inpi.md) : Exploration du RNE (API, authentification, métadonnées, bilans saisis, actes PDF).
- [`inpi-ingestion.md`](./inpi-ingestion.md) : Architecture et implémentation du pipeline d'ingestion INPI RNE.
