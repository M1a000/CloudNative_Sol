package com.pedidos360.catalogo.messaging.kafka.inventario;

import com.pedidos360.catalogo.config.KafkaTopicosProperties;
import com.pedidos360.catalogo.dto.kafka.InventarioActualizadoEvent;
import com.pedidos360.catalogo.service.InventarioEventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Caso de uso 2 (sincronizacion de inventario) -> productor del topico pedidos360.inventario.actualizado.
 *
 * <ul>
 *   <li>Key = productoId: todos los cambios de un producto van a la misma particion, en orden,
 *       y la compactacion conserva solo el ultimo.</li>
 *   <li>Se publica DESPUES del commit de la transaccion: si la BD hace rollback (por ejemplo, stock
 *       insuficiente), no se anuncia un cambio que nunca ocurrio.</li>
 *   <li>El envio es asincrono; el resultado (particion/offset o error) queda en el log. Si Kafka
 *       no esta disponible, la operacion de negocio no falla: el evento se pierde y se registra.</li>
 * </ul>
 */
@Component
public class KafkaInventarioEventPublisher implements InventarioEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaInventarioEventPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topico;

    public KafkaInventarioEventPublisher(KafkaTemplate<String, Object> kafkaTemplate, KafkaTopicosProperties props) {
        this.kafkaTemplate = kafkaTemplate;
        this.topico = props.topicos().inventarioActualizado().nombre();
    }

    @Override
    public void publicar(InventarioActualizadoEvent evento) {
        ejecutarTrasCommit(() -> enviar(String.valueOf(evento.productoId()), evento,
                evento.tipoEvento() + " producto " + evento.productoId()
                        + " (stock " + evento.stockAnterior() + " -> " + evento.stockActual() + ")"));
    }

    @Override
    public void publicarEliminacion(Long productoId) {
        ejecutarTrasCommit(() -> enviar(String.valueOf(productoId), null,
                "TOMBSTONE producto " + productoId + " eliminado"));
    }

    private void enviar(String key, InventarioActualizadoEvent evento, String descripcion) {
        try {
            kafkaTemplate.send(topico, key, evento).whenComplete((resultado, error) -> {
                if (error == null) {
                    var meta = resultado.getRecordMetadata();
                    log.info("[KAFKA-PRODUCTOR] {} publicado en {}-{}@{} (key={})",
                            descripcion, meta.topic(), meta.partition(), meta.offset(), key);
                } else {
                    log.error("[KAFKA-PRODUCTOR] No se pudo publicar {} en {}: {}",
                            descripcion, topico, error.getMessage());
                }
            });
        } catch (Exception e) {
            // Kafka caido: send() falla al pedir metadata (max.block.ms). No se interrumpe el negocio.
            log.error("[KAFKA-PRODUCTOR] Kafka no disponible, no se publico {}: {}", descripcion, e.getMessage());
        }
    }

    /** Si hay una transaccion activa, espera al commit; si no, envia de inmediato. */
    private void ejecutarTrasCommit(Runnable envio) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    envio.run();
                }
            });
        } else {
            envio.run();
        }
    }
}
