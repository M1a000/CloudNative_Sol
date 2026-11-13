#!/bin/bash
# Compila los 4 microservicios y los (re)levanta junto al cluster RabbitMQ, Kafka
# (Zookeeper + 3 brokers + Kafka-UI) y MySQL en la EC2.
# Lo ejecuta arranque.sh al crear la instancia. Para actualizar despues de un push:
#   cd /opt/pedidos360 && sudo git pull && sudo bash infraestructura/desplegar.sh
set -eu

RAIZ="$(cd "$(dirname "$0")/.." && pwd)"

# Compila los .jar con Maven dentro de un contenedor (la EC2 no necesita Java ni Maven instalados).
# Los tests se corren en el computador de desarrollo (mvn clean test), no en el despliegue.
docker run --rm -v "$RAIZ/back-app":/app -v maven_repo:/root/.m2 -w /app \
  maven:3.9-eclipse-temurin-21 mvn -q -B clean package -DskipTests

# La IP publica cambia en cada despliegue: se consulta a AWS para autorizarla en CORS.
TOKEN="$(curl -s -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')"
IP_PUBLICA="$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/public-ipv4)"
export CORS_ORIGINS="http://localhost:4200,http://$IP_PUBLICA:4200"

cd "$RAIZ/infraestructura"
docker compose up -d --build

echo "Backend desplegado en http://$IP_PUBLICA"
echo "  8081 productos-catalogo · 8082 venta-carrito · 8083 rabbitmq-admin · 8084 kafka-admin"
echo "  15672 RabbitMQ (admin/admin123) · 8090 Kafka-UI"
