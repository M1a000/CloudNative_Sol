package com.pedidos360.kafkaadmin.dto;

/** Una configuracion de topico con su valor efectivo y de donde proviene. */
public record ConfiguracionResponse(String nombre, String valor, String origen, boolean porDefecto,
                                    boolean soloLectura) {
}
