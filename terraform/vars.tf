variable "raw_data_bucket_name" {
  type    = string
  default = "totesys-raw-data-aci"
}

variable "dates_bucket_name" {
  type    = string
  default = "raw-data-dates-aci"
}

variable "python_runtime" {
  type    = string
  default = "python3.12"
}
