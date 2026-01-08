
resource "aws_secretsmanager_secret" "star_db" {
  name = "orpiment_db_secret"
}

resource "aws_secretsmanager_secret_version" "star_db_attach" {
  secret_id = aws_secretsmanager_secret.star_db.id

  secret_string = jsonencode({
    warehouse_user     = var.db_username
    warehouse_password = var.db_password
    warehouse_database = aws_db_instance.aci_db.db_name
    warehouse_host     = aws_db_instance.aci_db.address
    warehouse_port     = tostring(aws_db_instance.aci_db.port)
  })
}
