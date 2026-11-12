package com.pedidos360.catalogo.messaging.kafka.ventas;

import com.pedidos360.catalogo.dto.kafka.VentaConfirmadaEvent;
import com.pedidos360.catalogo.messaging.comun.ValidadorMensajes;
import com.pedidos360.catalogo.service.MetricasVentaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Caso de uso 1 (analitica de ventas) -> consumidor del topico pedidos360.ventas.confirmadas.
 *
 * Consumer group "catalogo-metricas-ventas" con 3 hilos (uno por particion): Kafka reparte las
 * particiones entre los hilos y, si se levanta otra instancia del servicio, entre las instancias.
 *
 * Esta clase solo hace mensajeria (leer, validar, registrar en el log); la logica de negocio esta
 * en {@link MetricasVentaService}. Si algo falla, la excepcion sube al DefaultErrorHandler
 * de KafkaConfig, que decide entre reintentar o enviar al DLT.
 */
@Component
public class VentasConfirmadasConsumer {

    private static final Logger log = LoggerFactory.getLogger(VentasConfirmadasConsumer.class);

    private final MetricasVentaService metricasVentaService;
    private final ValidadorMensajes validador;

    public VentasConfirmadasConsumer(MetricasVentaService metricasVentaService, ValidadorMensajes validador) {
        this.metricasVentaService = metricasVentaService;
        this.validador = validador;
    }

    @KafkaListener(
            id = "metricas-ventas-consumer",
            topics = "${pedidos360.kafka.topicos.ventas-confirmadas.nombre}",
            groupId = "${pedidos360.kafka.consumidores.metricas-ventas.grupo}",
            concurrency = "${pedidos360.kafka.consumidores.metricas-ventas.concurrencia}",
            properties = "spring.json.value.default.type=com.pedidos360.catalogo.dto.kafka.VentaConfirmadaEvent")
    public void alRecibirVenta(ConsumerRecord<String, VentaConfirmadaEvent> registro) {
        log.info("[KAFKA-CONSUMIDOR] Venta recibida de {}-{}@{} (key={})",
                registro.topic(), registro.partition(), registro.offset(), registro.key());

        VentaConfirmadaEvent venta = validador.validar(registro.value());

        boolean aplicada = metricasVentaService.registrarVenta(venta, registro.topic());
        if (aplicada) {
            log.info("[KAFKA-CONSUMIDOR] OK pedido {}: {} unidad(es), total {} {} -> metricas actualizadas",
                    venta.pedidoId(), venta.cantidadUnidades(), venta.total(), venta.moneda());
        } else {
            log.info("[KAFKA-CONSUMIDOR] Evento {} (pedido {}) ya procesado -> se ignora (idempotencia)",
                    venta.eventoId(), venta.pedidoId());
        }
    }
}
