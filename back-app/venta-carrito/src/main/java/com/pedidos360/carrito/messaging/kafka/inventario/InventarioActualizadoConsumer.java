package com.pedidos360.carrito.messaging.kafka.inventario;

import com.pedidos360.carrito.dto.kafka.InventarioActualizadoEvent;
import com.pedidos360.carrito.messaging.comun.MensajeInvalidoException;
import com.pedidos360.carrito.messaging.comun.ValidadorMensajes;
import com.pedidos360.carrito.service.SincronizacionInventarioService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Caso de uso 2 (sincronizacion de inventario) -> consumidor del topico pedidos360.inventario.actualizado.
 *
 * Consumer group "carrito-sync-inventario" con 3 hilos (uno por particion). Como la key es el
 * productoId, todos los cambios de un mismo producto los procesa siempre el mismo hilo, en orden.
 *
 * Esta clase solo hace mensajeria; la logica esta en {@link SincronizacionInventarioService}.
 * Los errores suben al DefaultErrorHandler de KafkaConfig (reintentos o DLT).
 */
@Component
public class InventarioActualizadoConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventarioActualizadoConsumer.class);

    private final SincronizacionInventarioService sincronizacionService;
    private final ValidadorMensajes validador;

    public InventarioActualizadoConsumer(SincronizacionInventarioService sincronizacionService,
                                         ValidadorMensajes validador) {
        this.sincronizacionService = sincronizacionService;
        this.validador = validador;
    }

    @KafkaListener(
            id = "sync-inventario-consumer",
            topics = "${pedidos360.kafka.topicos.inventario-actualizado.nombre}",
            groupId = "${pedidos360.kafka.consumidores.sync-inventario.grupo}",
            concurrency = "${pedidos360.kafka.consumidores.sync-inventario.concurrencia}",
            properties = "spring.json.value.default.type=com.pedidos360.carrito.dto.kafka.InventarioActualizadoEvent")
    public void alRecibirCambioInventario(ConsumerRecord<String, InventarioActualizadoEvent> registro) {
        log.info("[KAFKA-CONSUMIDOR] Cambio de inventario recibido de {}-{}@{} (key={})",
                registro.topic(), registro.partition(), registro.offset(), registro.key());

        // Tombstone (valor null): el producto fue eliminado del catalogo
        if (registro.value() == null) {
            Long productoId = productoIdDesdeKey(registro.key());
            int afectados = sincronizacionService.marcarEliminado(productoId);
            log.info("[KAFKA-CONSUMIDOR] Producto {} eliminado del catalogo -> {} item(s) marcados sin stock",
                    productoId, afectados);
            return;
        }

        InventarioActualizadoEvent evento = validador.validar(registro.value());
        if (!String.valueOf(evento.productoId()).equals(registro.key())) {
            throw new MensajeInvalidoException("La key " + registro.key()
                    + " no coincide con productoId " + evento.productoId());
        }

        SincronizacionInventarioService.Resultado resultado = sincronizacionService.aplicar(evento);
        log.info("[KAFKA-CONSUMIDOR] OK {} producto {} (precio {}, stock {}, disponible {}) -> "
                        + "{} item(s) de carrito actualizados, {} descartados por evento atrasado",
                evento.tipoEvento(), evento.productoId(), evento.precio(), evento.stockActual(),
                evento.disponible(), resultado.actualizados(), resultado.descartados());
    }

    private Long productoIdDesdeKey(String key) {
        try {
            return Long.valueOf(key);
        } catch (NumberFormatException | NullPointerException e) {
            throw new MensajeInvalidoException("Tombstone con key invalida: " + key);
        }
    }
}
