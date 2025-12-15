# variable "raw_data_bucket_name" {
#   type    = string
#   default = "totesys-raw-data-aci"
# }

# variable "lambda_code_bucket_name" {
#   type    = string
#   default = "lambda-func-code-aci"
# }

# variable "extract_lambda_func_name" {
#   type    = string
#   default = "extract-raw-data-lambda-func"
# }

# variable "clean_data_bucket_name" {
#   type    = string
#   default = "raw-data-dates-aci"
# }

variable "python_runtime" {
  type    = string
  default = "python3.12"
}

# variable "libraries_layer_bucket_name" {
#   type    = string
#   default = "libraries-layer-aci"
# }
