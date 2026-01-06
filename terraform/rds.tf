resource "aws_db_instance" "aci_db" {
  allocated_storage = 20
  storage_type = "gp2"
  engine = "postgres"
  engine_version = "16"
  auto_minor_version_upgrade = true
  instance_class = "db.t4g.micro"
  identifier = "aci-db"
  username = var.db_username
  password = var.db_password

  vpc_security_group_ids = [aws_security_group.rds_sg.id]
  db_subnet_group_name = aws_db_subnet_group.db_subnet_group.name

  backup_retention_period = 7 
  backup_window = "03:00-04:00"
  maintenance_window = "mon:04:00-mon:04:30"
  
  # Enable automated backups
  skip_final_snapshot = false
  final_snapshot_identifier = "aci-db-${timestamp()}"

}

# VPC
resource "aws_vpc" "vpc" {
  cidr_block = "10.0.0.0/16"
}

resource "aws_security_group" "rds_sg" {
  name_prefix = "rds-"

  vpc_id = aws_vpc.vpc.id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }

  egress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }
}

# RDS Subnet group
resource "aws_subnet" "subnet_a" {
  vpc_id     = aws_vpc.my_vpc.id
  cidr_block = "10.0.1.0/24"
  availability_zone = "eu-west-2"
}

resource "aws_subnet" "subnet_b" {
  vpc_id     = aws_vpc.my_vpc.id
  cidr_block = "10.0.2.0/24"
  availability_zone = "eu-west-2"
}

resource "aws_db_subnet_group" "db_subnet_group" {
  name = "aci-db-subnet-group"
  subnet_ids = [aws_subnet.subnet_a.id, aws_subnet.subnet_b.id]

  tags = {
    Name = "ACI DB Subnet Group"
  }
}
