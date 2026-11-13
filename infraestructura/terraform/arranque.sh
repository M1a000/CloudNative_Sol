#!/bin/bash
# Script de arranque de la instancia (user_data): se ejecuta una sola vez, como root,
# la primera vez que la maquina enciende. Instala Docker, clona el repositorio y
# levanta RabbitMQ, Kafka, MySQL y los 4 microservicios con infraestructura/desplegar.sh.
# El log queda en /var/log/arranque.log
exec > /var/log/arranque.log 2>&1
set -eux

# Swap de 2 GB como respaldo: evita que el kernel mate contenedores en un peak de memoria
# (compilacion con Maven + arranque simultaneo de los brokers y las JVM).
if [ ! -f /swapfile ]; then
  fallocate -l 2G /swapfile
  chmod 600 /swapfile
  mkswap /swapfile
  swapon /swapfile
  echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

# Docker (incluye el plugin "docker compose")
curl -fsSL https://get.docker.com | sh
systemctl enable --now docker
usermod -aG docker ubuntu || true

# Codigo del proyecto
git clone --depth 1 --branch "${rama}" "${repo_url}" /opt/pedidos360

bash /opt/pedidos360/infraestructura/desplegar.sh

echo "ARRANQUE COMPLETO"
