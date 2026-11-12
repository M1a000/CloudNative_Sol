#!/bin/bash
# =============================================================================
# Crea (o actualiza) los tópicos de Pedidos360 con la configuración de cada flujo.
# Lo ejecuta el contenedor kafka-init de docker/kafka.yml, pero también se puede
# correr a mano:  docker compose run --rm kafka-init
#
# Es idempotente: si el tópico ya existe, solo vuelve a aplicar sus configuraciones.
# Justificación de cada valor: docs/kafka/casos-de-uso-kafka.md
# =============================================================================
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka1:29092,kafka2:29092,kafka3:29092}"
REPLICAS="${REPLICAS:-3}"
MIN_ISR="${MIN_ISR:-2}"

crear_topico() {
  local nombre="$1" particiones="$2" configs="$3"

  if kafka-topics --bootstrap-server "$BOOTSTRAP" --list | grep -qx "$nombre"; then
    echo "[=] $nombre ya existe -> se aplican sus configuraciones"
    kafka-configs --bootstrap-server "$BOOTSTRAP" --alter --entity-type topics \
      --entity-name "$nombre" --add-config "$configs"
  else
    echo "[+] Creando $nombre (particiones=$particiones, replicas=$REPLICAS)"
    local args=()
    IFS=',' read -ra pares <<< "$configs"
    for par in "${pares[@]}"; do args+=(--config "$par"); done
    kafka-topics --bootstrap-server "$BOOTSTRAP" --create --topic "$nombre" \
      --partitions "$particiones" --replication-factor "$REPLICAS" "${args[@]}"
  fi
}

echo "Esperando a que el clúster responda en $BOOTSTRAP ..."
until kafka-topics --bootstrap-server "$BOOTSTRAP" --list > /dev/null 2>&1; do sleep 2; done

# Caso de uso 1: analítica de ventas. Hechos históricos -> se borran por antigüedad (7 días).
crear_topico "pedidos360.ventas.confirmadas" 3 \
  "cleanup.policy=delete,retention.ms=604800000,min.insync.replicas=$MIN_ISR"

# Caso de uso 2: estado del inventario. Solo importa el último estado por productoId -> compactación.
crear_topico "pedidos360.inventario.actualizado" 3 \
  "cleanup.policy=compact,min.compaction.lag.ms=60000,segment.ms=3600000,delete.retention.ms=86400000,min.cleanable.dirty.ratio=0.1,min.insync.replicas=$MIN_ISR"

# Dead Letter Topics: mensajes que no se pudieron procesar, 14 días para análisis.
crear_topico "pedidos360.ventas.confirmadas.DLT" 1 \
  "cleanup.policy=delete,retention.ms=1209600000,min.insync.replicas=$MIN_ISR"
crear_topico "pedidos360.inventario.actualizado.DLT" 1 \
  "cleanup.policy=delete,retention.ms=1209600000,min.insync.replicas=$MIN_ISR"

echo
echo "Tópicos del clúster:"
kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic 'pedidos360.*' | grep -v "^\s*Topic:.*Partition:" || true
