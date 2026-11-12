package com.pedidos360.catalogo.messaging.pedidos;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pedidos360.catalogo.exception.StockInsuficienteException;
import com.pedidos360.catalogo.messaging.comun.EjecutorConReintentos;
import com.pedidos360.catalogo.messaging.comun.LectorMensajes;
import com.pedidos360.catalogo.service.ProductoService;
import com.rabbitmq.client.Channel;
import jakarta.validation.Validation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.dao.DataAccessResourceFailureException;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Verifica las decisiones de confirmacion del consumidor: ACK, reintento, y NACK hacia la DLQ.
 * No necesita RabbitMQ: el canal es un mock y se comprueba que metodo se le llamo.
 */
@ExtendWith(MockitoExtension.class)
class StockPedidoConsumerTest {

    private static final long DELIVERY_TAG = 7L;

    private static final String PEDIDO_VALIDO = """
            {"pedidoId":"PED-1","usuarioId":"user-123","total":16470.0,
             "fechaCreacion":"2026-10-12T13:00:00",
             "items":[
               {"productoId":1,"nombreProducto":"Hamburguesa","cantidad":2,"precioUnitario":4990.0},
               {"productoId":1,"nombreProducto":"Hamburguesa","cantidad":1,"precioUnitario":4990.0},
               {"productoId":3,"nombreProducto":"Bebida","cantidad":1,"precioUnitario":1500.0}]}
            """;

    @Mock
    private ProductoService productoService;

    @Mock
    private Channel canal;

    private StockPedidoConsumer consumer;

    @BeforeEach
    void setUp() {
        LectorMensajes lector = new LectorMensajes(
                new ObjectMapper().findAndRegisterModules(),
                Validation.buildDefaultValidatorFactory().getValidator());
        // 3 intentos, 1 ms de espera para que el test sea rapido
        EjecutorConReintentos reintentos = new EjecutorConReintentos(3, 1, 1.0);
        consumer = new StockPedidoConsumer(productoService, lector, reintentos);
    }

    private Message mensaje(String cuerpo) {
        MessageProperties propiedades = new MessageProperties();
        propiedades.setDeliveryTag(DELIVERY_TAG);
        return new Message(cuerpo.getBytes(StandardCharsets.UTF_8), propiedades);
    }

    @Test
    void pedidoValidoDescuentaElStockAgrupadoYHaceAck() throws Exception {
        consumer.alRecibirPedidoCreado(mensaje(PEDIDO_VALIDO), canal);

        verify(productoService).descontarStock(eq(Map.of(1L, 3, 3L, 1)), anyString());
        verify(canal).basicAck(DELIVERY_TAG, false);
        verify(canal, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
    }

    @Test
    void jsonMalFormadoVaDirectoALaDlqSinLlamarAlServicio() throws Exception {
        consumer.alRecibirPedidoCreado(mensaje("esto no es json"), canal);

        verifyNoInteractions(productoService);
        verify(canal).basicNack(DELIVERY_TAG, false, false);
        verify(canal, never()).basicAck(anyLong(), anyBoolean());
    }

    @Test
    void pedidoSinProductosEsInvalidoYVaALaDlq() throws Exception {
        consumer.alRecibirPedidoCreado(mensaje("""
                {"pedidoId":"PED-2","usuarioId":"user-123","items":[]}
                """), canal);

        verifyNoInteractions(productoService);
        verify(canal).basicNack(DELIVERY_TAG, false, false);
    }

    @Test
    void stockInsuficienteNoSeReintentaYVaALaDlq() throws Exception {
        doThrow(new StockInsuficienteException(1L, 0, 3)).when(productoService).descontarStock(anyMap(), anyString());

        consumer.alRecibirPedidoCreado(mensaje(PEDIDO_VALIDO), canal);

        verify(productoService, times(1)).descontarStock(anyMap(), anyString());
        verify(canal).basicNack(DELIVERY_TAG, false, false);
    }

    @Test
    void errorRecuperableSeReintentaTresVecesYLuegoVaALaDlq() throws Exception {
        doThrow(new DataAccessResourceFailureException("BD no disponible"))
                .when(productoService).descontarStock(anyMap(), anyString());

        consumer.alRecibirPedidoCreado(mensaje(PEDIDO_VALIDO), canal);

        verify(productoService, times(3)).descontarStock(anyMap(), anyString());
        verify(canal).basicNack(DELIVERY_TAG, false, false);
        verify(canal, never()).basicAck(anyLong(), anyBoolean());
    }

    @Test
    void errorRecuperableQueSeResuelveEnElSegundoIntentoTerminaEnAck() throws Exception {
        doThrow(new DataAccessResourceFailureException("BD no disponible"))
                .doNothing()
                .when(productoService).descontarStock(anyMap(), anyString());

        consumer.alRecibirPedidoCreado(mensaje(PEDIDO_VALIDO), canal);

        verify(productoService, times(2)).descontarStock(anyMap(), anyString());
        verify(canal).basicAck(DELIVERY_TAG, false);
    }
}
