# Documentation — Industrial Intelligence

Ce dossier regroupe la documentation technique, fonctionnelle et d'architecture des données du projet **Industrial Intelligence**.

---

## 📚 Référentiel des Sources de Données

### 🏢 Base SIRENE (INSEE / data.gouv.fr)

La base SIRENE constitue le référentiel pivot d'identification et de géolocalisation des entreprises et établissements français :

1. [`sirene_stock_unite_legale.md`](./sirene_stock_unite_legale.md) : Analyse exhaustive du fichier `stock-stockunitelegale-parquet.parquet` (30 020 346 entreprises, clé primaire `siren`, 35 colonnes).
2. [`sirene_stock_etablissement.md`](./sirene_stock_etablissement.md) : Analyse exhaustive du fichier `stock-stocketablissement-parquet.parquet` (44 064 115 implantations physiques, clé `siret`, coordonnées Lambert 93, codes communes INSEE, 52 colonnes).
3. [`sirene_stock_etablissement_historique.md`](./sirene_stock_etablissement_historique.md) : Analyse du fichier `stock-stocketablissementhistorique-parquet.parquet` (96 228 441 tranches temporelles SCD2, suivi des fermetures, reconversions sectorielles et changements d'activité, 18 colonnes).
4. [`sirene_interconnexions_et_modelisation.md`](./sirene_interconnexions_et_modelisation.md) : Architecture de jointure interne (1:N, siège 1:1, SCD2), croisements externes (INPI RNE bilans & actes, BODACC, BOAMP, COG) et modèle dimensionnel / graphe cible pour GCS et BigQuery.
