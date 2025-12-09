variable "raw_data_bucket_name" {
  type    = string
  default = "totesys-raw-data-aci"
}

# variable "dates_bucket_name" {
#   type    = string
#   default = "raw-data-dates-aci"
# }

variable "lambda_code_bucket_name" {
  type    = string
  default = "lamda-function-code-aci"
}

variable "extract_lambda_func_name" {
  type    = string
  default = "extract-raw-data-lambda-func"
}

variable "clean_data_bucket_name" {
  type    = string
  default = "clean-data-aci"
}