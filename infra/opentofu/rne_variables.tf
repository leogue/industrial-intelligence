variable "enable_rne_prerequisites" {
  description = "Créer le dépôt d'images et les secrets vides avant publication de l'image et ajout des valeurs (ressources facturables)."
  type        = bool
  default     = false
}

variable "enable_rne_ingestion" {
  description = "Définir les jobs, secrets vides, comptes de service et IAM RNE. Aucun job n'est exécuté par apply."
  type        = bool
  default     = false
}

variable "rne_region" {
  description = "Région des jobs, du registre et du Scheduler ; ne modifie pas la localisation du bucket."
  type        = string
  default     = "europe-west9"
}

variable "rne_image" {
  description = "Image publiée, épinglée par digest : REGION-docker.pkg.dev/PROJECT/rne/ingestion@sha256:..."
  type        = string
  default     = ""
  validation {
    condition     = var.rne_image == "" || can(regex("^[^[:space:]]+@sha256:[a-f0-9]{64}$", var.rne_image))
    error_message = "Utiliser une image immuable par digest, pas un tag latest."
  }
}

variable "rne_prefix" {
  description = "Namespace des objets : raw/PREFIX/ et control/PREFIX/. Pas de slash initial/final."
  type        = string
  default     = "inpi/rne"
  validation {
    condition     = can(regex("^[a-zA-Z0-9_-]+(/[a-zA-Z0-9_-]+)*$", var.rne_prefix))
    error_message = "Le préfixe doit contenir des composants de chemin simples."
  }
}

variable "rne_stock_file" {
  description = "Nom explicite du ZIP de formalités à télécharger ; pas d'URL ni d'identifiants."
  type        = string
  default     = ""
}

variable "rne_sync_start" {
  description = "Borne supposée du stock RNE (ISO 8601 UTC) : le différentiel collecte à partir de cette date. Ne pas la changer une fois le checkpoint créé."
  type        = string
  default     = ""
  validation {
    condition     = var.rne_sync_start == "" || can(regex("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$", var.rne_sync_start))
    error_message = "Format attendu : AAAA-MM-JJTHH:MM:SSZ (UTC)."
  }
}

variable "rne_schedule_enabled" {
  description = "Activer le job quotidien sync après publication et essai de l'image."
  type        = bool
  default     = false
}

variable "enable_rne_materialization" {
  description = "Créer le job manuel ZIP vers Parquet (sans secret INPI). Désactivé par défaut."
  type        = bool
  default     = false
}

variable "rne_materialize_image" {
  description = "Image contenant la commande materialize, épinglée par digest ; indépendante du rollout des jobs d'ingestion."
  type        = string
  default     = ""
  validation {
    condition     = var.rne_materialize_image == "" || can(regex("^[^[:space:]]+@sha256:[a-f0-9]{64}$", var.rne_materialize_image))
    error_message = "Utiliser une image immuable par digest."
  }
}

variable "rne_stock_revision" {
  description = "Empreinte distante du reçu de stock (64 caractères hexadécimaux), pas son SHA-256 de contenu."
  type        = string
  default     = ""
  validation {
    condition     = var.rne_stock_revision == "" || can(regex("^[a-f0-9]{64}$", var.rne_stock_revision))
    error_message = "Utiliser la révision du reçu de stock validé."
  }
}

variable "rne_schedule" {
  description = "Cron du job sync (UTC). Une exécution déjà à jour se termine sans appel API ; sync garde un jour de marge de publication."
  type        = string
  default     = "0 5 * * *"
}
