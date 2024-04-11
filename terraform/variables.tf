variable "credentials" {
  description = "My Credentials"
  default     = "google_credentials.json"
}

variable "project" {
  type        = string
  description = "GCP project ID"
  default     = "book-data-pipeline"
}

variable "region" {
  type        = string
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "asia-southeast1"
}

variable "storage_class" {
  type        = string
  description = "The Storage Class of the new bucket. Ref: https://cloud.google.com/storage/docs/storage-classes"
  default     = "STANDARD"
}

variable "book_ext_datasets" {
  type        = string
  description = "Dataset in BigQuery where raw data (external tables) will be loaded."
  default     = "book_ext"
}

variable "book_analytics_datasets" {
  type        = string
  description = "Dataset in BigQuery where raw data (from Google Cloud Storage and DBT) will be loaded."
  default     = "book_analytics"
}
