package com.pedidos360.carrito.service;

import com.pedidos360.carrito.dto.kafka.VentaConfirmadaEvent;

/**
 * Lo unico que la logica de negocio sabe de Kafka: "avisa que se confirmo una venta".
 * La implementacion esta en messaging.kafka.ventas.KafkaVentaEventPublisher.
 */
public interface VentaEventPublisher {

    void publicarVentaConfirmada(VentaConfirmadaEvent evento);
}
