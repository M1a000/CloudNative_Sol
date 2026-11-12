package com.pedidos360.kafkaadmin.dto;

/** Hacia donde se mueven los offsets de un consumer group. */
public enum EstrategiaReset {
    /** Al inicio de la retencion: el grupo vuelve a procesar todos los mensajes conservados. */
    EARLIEST,
    /** Al final: el grupo ignora lo pendiente y solo procesa mensajes nuevos. */
    LATEST
}
