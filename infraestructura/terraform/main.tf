# IP publica del equipo que ejecuta "terraform apply". Se detecta sola para no
# tener que escribirla en cada despliegue; solo esa IP puede entrar a la instancia.
data "http" "mi_ip" {
  url = "https://checkip.amazonaws.com"
}

locals {
  allowed_cidr = var.allowed_cidr != "" ? var.allowed_cidr : "${chomp(data.http.mi_ip.response_body)}/32"
}

resource "aws_security_group" "sg_microservicios" {
  name        = "${var.proyecto}-sg"
  description = "Grupo de seguridad para microservicios y bases de datos"
  vpc_id      = aws_vpc.microservicios_vpc.id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [local.allowed_cidr]
  }

  ingress {
    from_port   = 8080
    to_port     = 8090
    protocol    = "tcp"
    cidr_blocks = [local.allowed_cidr]
  }

  ingress {
    from_port   = 3306
    to_port     = 3306
    protocol    = "tcp"
    cidr_blocks = [local.allowed_cidr]
  }

  ingress {
    from_port   = 5672
    to_port     = 5672
    protocol    = "tcp"
    cidr_blocks = [local.allowed_cidr]
  }

  ingress {
    from_port   = 15672
    to_port     = 15672
    protocol    = "tcp"
    cidr_blocks = [local.allowed_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.proyecto}-sg"
  }
}

resource "aws_vpc" "microservicios_vpc" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "${var.proyecto}-vpc"
  }
}

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.microservicios_vpc.id

  tags = {
    Name = "${var.proyecto}-igw"
  }
}

resource "aws_subnet" "subred_publica" {
  vpc_id                  = aws_vpc.microservicios_vpc.id
  cidr_block              = "10.0.1.0/24"
  map_public_ip_on_launch = true
  availability_zone = "${var.aws_region}a"

  tags = {
    Name = "${var.proyecto}-subred-publica"
  }
}

resource "aws_route_table" "tabla_rutas_publica" {
  vpc_id = aws_vpc.microservicios_vpc.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }

  tags = {
    Name = "${var.proyecto}-rutas-publicas"
  }
}

resource "aws_route_table_association" "asociacion_publica" {
  subnet_id      = aws_subnet.subred_publica.id
  route_table_id = aws_route_table.tabla_rutas_publica.id
}

resource "aws_instance" "servidor_infraestructura" {
  ami                    = "ami-0c7217cdde317cfec"
  instance_type          = var.instance_type
  subnet_id              = aws_subnet.subred_publica.id
  vpc_security_group_ids = [aws_security_group.sg_microservicios.id]
  key_name               = var.key_name

  # Al crear la instancia: instala Docker, clona el repo y levanta RabbitMQ, MySQL y los 3 microservicios.
  # El replace evita que un salto de linea de Windows (CRLF) rompa el script en Linux.
  user_data                   = replace(templatefile("${path.module}/arranque.sh", { repo_url = var.repo_url, rama = var.rama }), "\r\n", "\n")
  user_data_replace_on_change = true

  # Espacio para las imagenes Docker (el disco por defecto de 8 GB queda justo)
  root_block_device {
    volume_size = 20
    volume_type = "gp3"
  }

  depends_on = [aws_route_table_association.asociacion_publica]

  tags = {
    Name = "${var.proyecto}-servidor"
  }
}

output "instance_public_ip" {
  description = "Dirección IPv4 pública de la instancia"
  value       = aws_instance.servidor_infraestructura.public_ip
}

output "rabbitmq_dashboard" {
  description = "Dashboard de RabbitMQ (admin / admin123). El despliegue completo tarda unos 8-10 minutos tras el apply"
  value       = "http://${aws_instance.servidor_infraestructura.public_ip}:15672"
}

output "ssh" {
  description = "Comando para entrar a la instancia"
  value       = "ssh -i ${var.key_name}.pem ubuntu@${aws_instance.servidor_infraestructura.public_ip}"
}