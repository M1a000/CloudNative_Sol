package com.pedidos360.catalogo.messaging.pedidos;

import com.pedidos360.catalogo.dto.PedidoCreadoEvent;
import com.pedidos360.catalogo.messaging.comun.ClasificadorErrores;
import com.pedidos360.catalogo.messaging.comun.EjecutorConReintentos;
import com.pedidos360.catalogo.messaging.comun.LectorMensajes;
import com.pedidos360.catalogo.service.ProductoService;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Dominio PEDIDOS -> consumidor del evento "pedido.creado" en productos-catalogo.
 * Descuenta del inventario las cantidades de cada producto del pedido.
 *
 * Confirmacion manual (acknowledge-mode: manual):
 * <pre>
 *  Resultado                                        Respuesta al broker
 *  -----------------------------------------------  ------------------------------------------
 *  Stock descontado                                 basicAck                -> sale de la cola
 *  Mensaje invalido o regla de negocio (sin stock)  basicNack(requeue=false) -> DLQ sin reintentar
 *  Error recuperable que persiste tras reintentos   basicNack(requeue=false) -> DLQ
 *  Servicio detenido en medio de un reintento       basicNack(requeue=true)  -> vuelve a la cola
 * </pre>
 * Nunca se reencola un mensaje que fallo por sus datos: generaria un bucle infinito ("poison message").
 */
@Component
public class StockPedidoConsumer {

    private static final Logger log = LoggerFactory.getLogger(StockPedidoConsumer.class);

    private final ProductoService productoService;
    private final LectorMensajes lector;
    private final EjecutorConReintentos reintentos;

    public StockPedidoConsumer(ProductoService productoService, LectorMensajes lector,
                               EjecutorConReintentos reintentos) {
        this.productoService = productoService;
        this.lector = lector;
        this.reintentos = reintentos;
    }

    @RabbitListener(id = "stock-pedido-consumer", queues = "${pedidos360.rabbitmq.colas.stock.nombre}")
    public void alRecibirPedidoCreado(Message mensaje, Channel canal) throws IOException {
        long deliveryTag = mensaje.getMessageProperties().getDeliveryTag();

        try {
            PedidoCreadoEvent pedido = lector.leer(mensaje, PedidoCreadoEvent.class);
            log.info("[STOCK] Pedido {} recibido con {} producto(s)", pedido.pedidoId(), pedido.items().size());

            Map<Long, Integer> cantidades = agruparCantidadesPorProducto(pedido);
            reintentos.ejecutar("STOCK pedido " + pedido.pedidoId(),
                    () -> productoService.descontarStock(cantidades, pedido.pedidoId()));

            canal.basicAck(deliveryTag, false);
            log.info("[STOCK] OK pedido {}: stock descontado -> ACK", pedido.pedidoId());

        } catch (InterruptedException e) {
            log.warn("[STOCK] Consumidor detenido durante un reintento -> NACK con requeue (lo tomara otra instancia)");
            canal.basicNack(deliveryTag, false, true);
            Thread.currentThread().interrupt();

        } catch (Exception e) {
            if (ClasificadorErrores.esNoRecuperable(e)) {
                log.error("[STOCK] Error NO recuperable: {} -> NACK sin reencolar -> DLQ. Mensaje: {}",
                        e.getMessage(), LectorMensajes.cuerpoComoTexto(mensaje));
            } else {
                log.error("[STOCK] Error recuperable que persistio tras {} intentos: {} -> NACK -> DLQ",
                        reintentos.getMaxIntentos(), e.getMessage(), e);
            }
            canal.basicNack(deliveryTag, false, false);
        }
    }

    /** Suma las cantidades si el mismo producto aparece en mas de una linea del pedido. */
    private Map<Long, Integer> agruparCantidadesPorProducto(PedidoCreadoEvent pedido) {
        Map<Long, Integer> cantidades = new LinkedHashMap<>();
        for (PedidoCreadoEvent.Item item : pedido.items()) {
            cantidades.merge(item.productoId(), item.cantidad(), Integer::sum);
        }
        return cantidades;
    }
}
