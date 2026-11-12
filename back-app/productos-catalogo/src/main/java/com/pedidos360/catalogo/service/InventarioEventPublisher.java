package com.pedidos360.catalogo.service;

import com.pedidos360.catalogo.dto.kafka.InventarioActualizadoEvent;

/**
 * Lo unico que la logica de negocio sabe de Kafka: "avisa que cambio el inventario".
 * La implementacion esta en messaging.kafka.inventario.KafkaInventarioEventPublisher,
 * asi ProductoService no depende de KafkaTemplate ni de nombres de topicos.
 */
public interface InventarioEventPublisher {

    /** Publica el estado completo del producto tras crearlo, actualizarlo o descontar stock. */
    void publicar(InventarioActualizadoEvent evento);

    /** Publica la eliminacion del producto (tombstone: key = productoId, valor null). */
    void publicarEliminacion(Long productoId);
}
