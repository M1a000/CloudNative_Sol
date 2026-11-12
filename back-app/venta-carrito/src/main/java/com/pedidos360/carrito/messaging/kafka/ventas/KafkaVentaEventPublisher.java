package com.pedidos360.carrito.messaging.kafka.ventas;

import com.pedidos360.carrito.config.KafkaTopicosProperties;
import com.pedidos360.carrito.dto.kafka.VentaConfirmadaEvent;
import com.pedidos360.carrito.service.VentaEventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Caso de uso 1 (analitica de ventas) -> productor del topico pedidos360.ventas.confirmadas.
 *
 * <ul>
 *   <li>Key = usuarioId: las ventas de un mismo cliente quedan en orden en la misma particion.</li>
 *   <li>Se publica DESPUES del commit: la venta ya quedo confirmada (y el pedido.creado ya salio por
 *       RabbitMQ). Si Kafka falla, la venta NO se revierte: la analitica es secundaria y el error
 *       queda en el log.</li>
 * </ul>
 */
@Component
public class KafkaVentaEventPublisher implements VentaEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaVentaEventPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topico;

    public KafkaVentaEventPublisher(KafkaTemplate<String, Object> kafkaTemplate, KafkaTopicosProperties props) {
        this.kafkaTemplate = kafkaTemplate;
        this.topico = props.topicos().ventasConfirmadas().nombre();
    }

    @Override
    public void publicarVentaConfirmada(VentaConfirmadaEvent evento) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    enviar(evento);
                }
            });
        } else {
            enviar(evento);
        }
    }

    private void enviar(VentaConfirmadaEvent evento) {
        String key = evento.usuarioId();
        try {
            kafkaTemplate.send(topico, key, evento).whenComplete((resultado, error) -> {
                if (error == null) {
                    var meta = resultado.getRecordMetadata();
                    log.info("[KAFKA-PRODUCTOR] Venta del pedido {} publicada en {}-{}@{} (key={}, {} unidad(es), total {})",
                            evento.pedidoId(), meta.topic(), meta.partition(), meta.offset(), key,
                            evento.cantidadUnidades(), evento.total());
                } else {
                    log.error("[KAFKA-PRODUCTOR] No se pudo publicar la venta del pedido {} en {}: {}",
                            evento.pedidoId(), topico, error.getMessage());
                }
            });
        } catch (Exception e) {
            log.error("[KAFKA-PRODUCTOR] Kafka no disponible, no se publico la venta del pedido {}: {}",
                    evento.pedidoId(), e.getMessage());
        }
    }
}
