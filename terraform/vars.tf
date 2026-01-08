variable "python_runtime" {
  type    = string
  default = "python3.12"
}

variable "db_username" {
  sensitive = true
}

variable "db_password" {
  sensitive = true
}
