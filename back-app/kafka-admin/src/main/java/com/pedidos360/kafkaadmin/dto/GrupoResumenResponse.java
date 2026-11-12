package com.pedidos360.kafkaadmin.dto;

/** Fila del listado de consumer groups. */
public record GrupoResumenResponse(String groupId, String estado, boolean simple) {
}
