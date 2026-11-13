package com.pedidos360.carrito.messaging.kafka;

import com.pedidos360.carrito.config.KafkaTopicosProperties;
import com.pedidos360.carrito.dto.PedidoCreadoEvent;
import com.pedidos360.carrito.dto.kafka.VentaConfirmadaEvent;
import com.pedidos360.carrito.messaging.kafka.ventas.KafkaVentaEventPublisher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Productor del caso de uso 1: DTO armado desde el pedido, topico, key y envio tras el commit. */
class KafkaVentaEventPublisherTest {

    private static final String TOPICO = "pedidos360.ventas.confirmadas";

    private KafkaTemplate<String, Object> kafkaTemplate;
    private KafkaVentaEventPublisher publisher;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        kafkaTemplate = mock(KafkaTemplate.class);
        KafkaTopicosProperties props = mock(KafkaTopicosProperties.class, RETURNS_DEEP_STUBS);
        when(props.topicos().ventasConfirmadas().nombre()).thenReturn(TOPICO);
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(new CompletableFuture<SendResult<String, Object>>());
        publisher = new KafkaVentaEventPublisher(kafkaTemplate, props);
    }

    @AfterEach
    void limpiarTransaccion() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    private static VentaConfirmadaEvent ventaDesdePedido() {
        PedidoCreadoEvent pedido = new PedidoCreadoEvent("PED-1", "usuario-1",
                List.of(new PedidoCreadoEvent.Item(1L, "Hamburguesa Clasica", 2, 5990.0),
                        new PedidoCreadoEvent.Item(4L, "Papas Fritas", 1, 2990.0)),
                14970.0, LocalDateTime.of(2026, 11, 13, 15, 8));
        return VentaConfirmadaEvent.desde(pedido);
    }

    @Test
    void elDtoSeArmaDesdeElPedidoConSubtotalesYTotales() {
        VentaConfirmadaEvent venta = ventaDesdePedido();

        assertThat(venta.tipoEvento()).isEqualTo(VentaConfirmadaEvent.TIPO);
        assertThat(venta.pedidoId()).isEqualTo("PED-1");
        assertThat(venta.cantidadUnidades()).isEqualTo(3);
        assertThat(venta.total()).isEqualTo(14970.0);
        assertThat(venta.items().get(0).subtotal()).isEqualTo(11980.0);
        assertThat(venta.eventoId()).isNotBlank();
    }

    @Test
    void publicaEnElTopicoDeVentasConKeyUsuarioId() {
        VentaConfirmadaEvent venta = ventaDesdePedido();

        publisher.publicarVentaConfirmada(venta);

        // key = usuarioId: las ventas de un cliente quedan en orden en la misma particion
        verify(kafkaTemplate).send(TOPICO, "usuario-1", venta);
    }

    @Test
    void dentroDeUnaTransaccionEsperaAlCommitParaPublicar() {
        TransactionSynchronizationManager.initSynchronization();
        VentaConfirmadaEvent venta = ventaDesdePedido();

        publisher.publicarVentaConfirmada(venta);
        verify(kafkaTemplate, never()).send(anyString(), anyString(), any());

        TransactionSynchronizationManager.getSynchronizations().forEach(TransactionSynchronization::afterCommit);
        verify(kafkaTemplate).send(TOPICO, "usuario-1", venta);
    }

    @Test
    void siKafkaNoEstaDisponibleLaConfirmacionDelPedidoNoFalla() {
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenThrow(new org.apache.kafka.common.errors.TimeoutException("sin brokers"));

        assertThatCode(() -> publisher.publicarVentaConfirmada(ventaDesdePedido())).doesNotThrowAnyException();
    }
}
