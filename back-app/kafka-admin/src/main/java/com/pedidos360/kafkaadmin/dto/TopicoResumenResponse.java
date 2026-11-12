package com.pedidos360.kafkaadmin.dto;

/** Fila del listado de topicos. */
public record TopicoResumenResponse(String nombre, int particiones, int factorReplicacion, boolean interno) {
}
