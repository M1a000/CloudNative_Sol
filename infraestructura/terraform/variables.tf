variable "aws_region" {
  description = "Región de AWS Academy"
  type        = string
  default     = "us-east-1"
}

variable "proyecto" {
  description = "Nombre del proyecto para las etiquetas"
  type        = string
  default     = "pedidos360"
}

variable "allowed_cidr" {
  description = "CIDR público autorizado para acceder a la instancia y sus servicios, por ejemplo 203.0.113.10/32. Vacío = se usa la IP pública de quien ejecuta terraform apply"
  type        = string
  default     = ""
}

variable "key_name" {
  description = "Nombre del Key Pair SSH existente en AWS"
  type        = string
  default     = "pedidos360"
}
variable "instance_type" {
  description = "Tipo de instancia. EP3: t3.large (2 vCPU, 8 GB) para el clúster RabbitMQ (3 nodos), Kafka (Zookeeper + 3 brokers), Kafka-UI, MySQL y 4 microservicios. t3.medium (4 GB) ya no alcanza"
  type        = string
  default     = "t3.large"
}

variable "repo_url" {
  description = "Repositorio público de GitHub que la instancia clona para desplegar el backend"
  type        = string
  default     = "https://github.com/M1a000/CloudNative_Sol.git"
}

variable "rama" {
  description = "Rama del repositorio que se despliega"
  type        = string
  default     = "main"
}
