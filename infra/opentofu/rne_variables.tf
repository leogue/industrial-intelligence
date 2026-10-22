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

variable "rne_initial_from" {
  description = "Borne différentielle initiale choisie après vérification de la couverture du stock (ISO 8601 avec fuseau). Jamais déduite du nom du ZIP."
  type        = string
  default     = ""
  validation {
    condition     = var.rne_initial_from == "" || can(formatdate("YYYY-MM-DD", var.rne_initial_from))
    error_message = "Utiliser une date RFC 3339 avec fuseau, ou laisser vide avant la préparation du bootstrap."
  }
}

variable "rne_schedule_enabled" {
  description = "Activer uniquement après bootstrap réussi, rattrapage manuel et contrôle de la couverture."
  type        = bool
  default     = false
}

variable "rne_schedule" {
  description = "Cron quotidien par défaut à 05:00 UTC ; refresh garde un jour de marge de publication."
  type        = string
  default     = "0 5 * * *"
}
