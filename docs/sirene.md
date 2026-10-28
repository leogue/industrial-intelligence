# SIRENE / data.gouv.fr — Exploration, Accès aux Données & Idempotence

> Exploration technique et contrat d'accès aux données du **22 octobre 2026**.
> Objectif : formaliser les accès API, les mécanismes de vérification par empreinte (hash/checksum) et la stratégie d'ingestion idempotente pour les fichiers Parquet SIRENE dans Google Cloud Storage (GCS).
> **Observé** = réponse HTTP/API et métadonnées réelles obtenues ; **documenté** = spécifications INSEE / data.gouv.fr ; **proposé** = architecture cible du projet.

---

## 1. Synthèse : Quelles APIs et données sont disponibles ?

Contrairement à l'INPI qui impose une authentification par session/JWT, la base SIRENE sur **data.gouv.fr** est en **accès ouvert (Open Data / Licence Ouverte v2.0)** sans clé d'API obligatoire pour le téléchargement des snapshots bulk.

| Service / API | Point d'entrée | Format | Cas d'usage pour Industrial Intelligence |
|---|---|---|---|
| **API Catalogue data.gouv.fr (Dataset)** | `GET https://www.data.gouv.fr/api/1/datasets/5b7ffc618b4c4169d30727e0/` | JSON | **Découverte des snapshots**, métadonnées de publication, URLs CDN directes, tailles et **empreintes SHA-1** pour l'idempotence. |
| **API Ressources data.gouv.fr** | `GET https://www.data.gouv.fr/api/1/datasets/{id}/resources/{resource_id}/` | JSON | Suivi unitaire d'un fichier spécifique (dernière modification, statut de conversion). |
| **Téléchargement Bulk (CDN static)** | `https://static.data.gouv.fr/resources/.../*.parquet` | Parquet (DuckDB) | **Ingestion mensuelle de masse** (Unités Légales, Établissements, Historiques) en streaming chunk par chunk. |
| **API Recherche d'Entreprises (DINUM)** | `GET https://recherche-entreprises.api.gouv.fr/search?q={query}` | JSON | Recherche rapide par mot-clé, raison sociale ou dirigeant ; requêtes ciblées sans authentification. |
| **API Sirene INSEE officielle (v3.11)** | `https://api.insee.fr/entreprises/sirene/V3.11/` | JSON / CSV | API officielle avec jeton OAuth2 (quota limité) pour les mises à jour unitaires en temps réel. *(Optionnelle, accès non confirmé pour l'équipe).* |

---

## 2. API Catalogue data.gouv.fr : Découverte et Métadonnées

### 2.1 Identifiants du jeu de données
- **URL API Dataset** : `https://www.data.gouv.fr/api/1/datasets/5b7ffc618b4c4169d30727e0/`
- **Identifiant Dataset** : `5b7ffc618b4c4169d30727e0`
- **Fréquence de mise à jour** : Mensuelle (au 1er de chaque mois).

### 2.2 Fichiers Parquet disponibles observés au 01/10/2026

Les fichiers sont convertis automatiquement en Parquet par data.gouv.fr à l'aide de **DuckDB** :

| Nom du Fichier | Resource ID (`resource.id`) | Taille (octets) | Empreinte SHA-1 (`resource.checksum.value`) | Lignes observées |
|---|---|---|---|---|
| **`stock-stockunitelegale-parquet.parquet`** | `350182c9-148a-46e0-8389-76c2ec1374a3` | 713 585 991 | `540f1ea166f6dad275cd5f9be6777dd0ae3df75b` | 30 020 346 |
| **`stock-stocketablissement-parquet.parquet`** | `a29c1297-1f92-4e2a-8f6b-8c902ce96c5f` | 2 220 887 978 | `03593e64ea6f810ee6243c40b675a9c1973e1f36` | 44 064 115 |
| **`stock-stocketablissementhistorique-parquet.parquet`** | `2b3a0c79-f97b-46b8-ac02-8be6c1f01a8c` | 877 620 853 | `7b409a263d54eeebed252a706ab1c6bcd133b15b` | 96 228 441 |
| **`stock-stockunitelegalehistorique-parquet.parquet`** | `1b9290ed-d0bc-461f-ba31-0250a99cc140` | 863 403 921 | `396d6647b88a04d172e7b80c15204756124fda5a` | Historique UL |
| **`stock-stocketablissementlienssuccession-parquet.parquet`** | `6ea8c0d2-a4b9-4ea2-ba63-7ec89c2dd4f4` | 119 372 997 | `33f373a964de8a68088a59850fdf29a2e77c461f` | Liens succession |

---

## 3. Stratégie d'Idempotence : Comment éviter de re-télécharger l'existant ?

Pour respecter les principes de [`AGENTS.md`](file:///home/mroyant/repository/industrial-intelligence/AGENTS.md) (pas de surcoût réseau/stockage GCP, idempotence stricte, pas de saturation mémoire) :

```mermaid
flowchart TD
    A["Interrogation API data.gouv.fr<br/><code>GET /api/1/datasets/5b7ffc618b4c4169d30727e0/</code>"] --> B["Extraction métadonnées par ressource<br/>(URL, checksum SHA-1, last_modified)"]
    B --> C{"Objet déjà présent dans GCS ?<br/><code>gs://...-data/raw/sirene/...</code>"}

    C -->|Oui| D{"Le hash stocké dans les métadonnées GCS<br/>est-il identique au SHA-1 distant ?"}
    D -->|Oui (Hash identique)| E["✅ SNAPSHOT DÉJÀ À JOUR<br/>Téléchargement ignoré (0 octet consommé)"]
    D -->|Non (Nouveau snapshot)| F["Téléchargement en streaming par chunks (8 Mo)<br/>+ Calcul SHA-1 à la volée"]

    C -->|Non (Inconnu)| F

    F --> G{"SHA-1 calculé == SHA-1 attendu ?"}
    G -->|Oui| H["Écriture GCS + Enregistrement métadonnées<br/>(sha1, last_modified, source_url)"]
    G -->|Non| I["❌ Erreur d'intégrité : rejet et alerte<br/>(Pas d'avancement de checkpoint)"]
    H --> J["Checkpoint validé"]
```

### 3.1 Structure du champ Checksum de l'API data.gouv.fr
Chaque objet `resource` de l'API renvoie une clé `checksum` normalisée :
```json
{
  "id": "350182c9-148a-46e0-8389-76c2ec1374a3",
  "title": "Sirene : Fichier StockUniteLegale - 01 octobre 2026 (format parquet)",
  "format": "parquet",
  "filesize": 713585991,
  "last_modified": "2026-10-01T07:33:54.372000+00:00",
  "checksum": {
    "type": "sha1",
    "value": "540f1ea166f6dad275cd5f9be6777dd0ae3df75b"
  },
  "url": "https://static.data.gouv.fr/resources/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/20261001-073325/stock-stockunitelegale-parquet.parquet"
}
```

### 3.2 Vérification HTTP préliminaire (en-têtes HEAD)
En complément de l'API, une requête HTTP `HEAD` sur l'URL directe du fichier CDN renvoie :
- `ETag` : hash HTTP du fichier sur le CDN.
- `Content-Length` : taille exacte en octets.
- `Last-Modified` : date de dernière modification HTTP.

---

## 4. Implémentation de Référence du Connecteur (Python)

Conforme au contrat de connecteur défini dans [`AGENTS.md`](file:///home/mroyant/repository/industrial-intelligence/AGENTS.md) Section 7 :
1. `discover()` : interroge l'API data.gouv.fr et filtre les Parquet.
2. `is_already_ingested()` : compare l'empreinte distante avec les métadonnées de l'objet GCS (`metadata["checksum_sha1"]`).
3. `ingest()` : streaming par chunks sans charger les 2,2 Go en RAM, calcul de hash à la volée et checkpoint validé uniquement après succès.

```python
"""Module d'ingestion idempotente des snapshots SIRENE Parquet depuis data.gouv.fr."""

import hashlib
import json
import logging
from typing import Any
import urllib.request
from google.cloud import storage

DATASET_API_URL = "https://www.data.gouv.fr/api/1/datasets/5b7ffc618b4c4169d30727e0/"
GCS_BUCKET_NAME = "industrial-intelligence-dev-data"
CHUNK_SIZE = 8 * 1024 * 1024  # 8 Mo par chunk en streaming


def discover_parquet_resources() -> list[dict[str, Any]]:
    """Découvre les ressources Parquet disponibles et leurs empreintes distantes."""
    req = urllib.request.Request(
        DATASET_API_URL, headers={"User-Agent": "Industrial-Intelligence-ETL/1.0"}
    )
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read().decode())

    resources = []
    for r in data.get("resources", []):
        fmt = (r.get("format") or "").lower()
        title = r.get("title", "").lower()
        if fmt == "parquet" or "parquet" in title:
            resources.append(
                {
                    "id": r["id"],
                    "title": r["title"],
                    "url": r["url"],
                    "filesize": r.get("filesize"),
                    "last_modified": r.get("last_modified"),
                    "checksum_type": (r.get("checksum") or {}).get("type"),
                    "checksum_value": (r.get("checksum") or {}).get("value"),
                }
            )
    return resources


def sync_resource_to_gcs(resource: dict[str, Any], storage_client: storage.Client) -> bool:
    """Télécharge la ressource en streaming vers GCS de manière idempotente."""
    filename = resource["url"].split("/")[-1]
    gcs_path = f"raw/sirene/{filename}"
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(gcs_path)

    # 1. Vérification d'idempotence via métadonnées GCS
    if blob.exists():
        blob.reload()
        existing_sha1 = (blob.metadata or {}).get("checksum_sha1")
        if existing_sha1 and existing_sha1 == resource["checksum_value"]:
            logging.info(f"[SKIP] {filename} est déjà à jour dans GCS (SHA-1: {existing_sha1}).")
            return False

    logging.info(f"[START] Téléchargement de {filename} ({resource['filesize']:,} octets)...")

    # 2. Streaming chunk par chunk et calcul du hash à la volée
    sha1 = hashlib.sha1()
    req = urllib.request.Request(
        resource["url"], headers={"User-Agent": "Industrial-Intelligence-ETL/1.0"}
    )

    with (
        urllib.request.urlopen(req) as response,
        blob.open("wb", content_type="application/vnd.apache.parquet") as gcs_file,
    ):
        while chunk := response.read(CHUNK_SIZE):
            sha1.update(chunk)
            gcs_file.write(chunk)

    computed_sha1 = sha1.hexdigest()

    # 3. Validation d'intégrité
    expected_sha1 = resource["checksum_value"]
    if expected_sha1 and computed_sha1 != expected_sha1:
        blob.delete()
        raise ValueError(
            f"Erreur d'intégrité pour {filename} : hash calculé ({computed_sha1}) != attendu ({expected_sha1})"
        )

    # 4. Enregistrement des métadonnées de provenance (checkpoint)
    blob.metadata = {
        "checksum_sha1": computed_sha1,
        "source_url": resource["url"],
        "source_resource_id": resource["id"],
        "data_gouv_last_modified": resource["last_modified"],
    }
    blob.patch()

    logging.info(f"[SUCCESS] {filename} ingéré avec succès dans gs://{GCS_BUCKET_NAME}/{gcs_path}")
    return True
```

---

## 5. Convention de Stockage GCS pour SIRENE

Conformément à la convention de nommage de [`AGENTS.md`](file:///home/mroyant/repository/industrial-intelligence/AGENTS.md) Section 6 :

```text
gs://industrial-intelligence-dev-data/
└── raw/sirene/
    ├── snapshot_date=2026-10-01/
    │   ├── stock-stockunitelegale-parquet.parquet
    │   ├── stock-stocketablissement-parquet.parquet
    │   ├── stock-stocketablissementhistorique-parquet.parquet
    │   └── manifest.json   <-- Contient les URLs, SHA-1 et métadonnées de l'API
```

Le fichier `manifest.json` stocké aux côtés des Parquet bruts contient l'empreinte complète de l'extraction API pour garantir une traçabilité et une reproductibilité totale de chaque snapshot.
