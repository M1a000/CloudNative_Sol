package com.pedidos360.catalogo.messaging.kafka;

import com.pedidos360.catalogo.dto.kafka.VentaConfirmadaEvent;
import com.pedidos360.catalogo.messaging.comun.MensajeInvalidoException;
import com.pedidos360.catalogo.messaging.comun.ValidadorMensajes;
import com.pedidos360.catalogo.messaging.kafka.ventas.VentasConfirmadasConsumer;
import com.pedidos360.catalogo.service.MetricasVentaService;
import jakarta.validation.Validation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * El consumidor solo valida y delega en el servicio de negocio. Los mensajes invalidos lanzan
 * MensajeInvalidoException, que el DefaultErrorHandler envia al DLT sin reintentar.
 */
class VentasConfirmadasConsumerTest {

    private static final String TOPICO = "pedidos360.ventas.confirmadas";

    private MetricasVentaService metricasVentaService;
    private VentasConfirmadasConsumer consumer;

    @BeforeEach
    void setUp() {
        metricasVentaService = mock(MetricasVentaService.class);
        ValidadorMensajes validador = new ValidadorMensajes(Validation.buildDefaultValidatorFactory().getValidator());
        consumer = new VentasConfirmadasConsumer(metricasVentaService, validador);
    }

    private static ConsumerRecord<String, VentaConfirmadaEvent> registro(VentaConfirmadaEvent venta) {
        return new ConsumerRecord<>(TOPICO, 1, 7L, "usuario-1", venta);
    }

    private static VentaConfirmadaEvent ventaValida() {
        return new VentaConfirmadaEvent("ev-1", VentaConfirmadaEvent.TIPO, 1, Instant.now(), "venta-carrito",
                "PED-1", "usuario-1",
                List.of(new VentaConfirmadaEvent.Item(1L, "Hamburguesa Clasica", 2, 5990.0, 11980.0)),
                2, 11980.0, "CLP");
    }

    @Test
    void unaVentaValidaSeDelegaAlServicioDeMetricas() {
        VentaConfirmadaEvent venta = ventaValida();
        when(metricasVentaService.registrarVenta(venta, TOPICO)).thenReturn(true);

        consumer.alRecibirVenta(registro(venta));

        verify(metricasVentaService).registrarVenta(venta, TOPICO);
    }

    @Test
    void unDuplicadoNoFallaElConsumidor() {
        VentaConfirmadaEvent venta = ventaValida();
        when(metricasVentaService.registrarVenta(venta, TOPICO)).thenReturn(false);

        consumer.alRecibirVenta(registro(venta));

        verify(metricasVentaService).registrarVenta(venta, TOPICO);
    }

    @Test
    void unaVentaConCantidadesNegativasEsInvalidaYNoLlegaAlServicio() {
        VentaConfirmadaEvent negativa = new VentaConfirmadaEvent("ev-2", VentaConfirmadaEvent.TIPO, 1, Instant.now(),
                "venta-carrito", "PED-2", "usuario-1",
                List.of(new VentaConfirmadaEvent.Item(1L, "Hamburguesa Clasica", -3, 5990.0, -17970.0)),
                -3, 0.0, "CLP");

        assertThatThrownBy(() -> consumer.alRecibirVenta(registro(negativa)))
                .isInstanceOf(MensajeInvalidoException.class)
                .hasMessageContaining("cantidad");
        verify(metricasVentaService, never()).registrarVenta(any(), anyString());
    }

    @Test
    void unMensajeVacioEsInvalido() {
        assertThatThrownBy(() -> consumer.alRecibirVenta(registro(null)))
                .isInstanceOf(MensajeInvalidoException.class);
        verify(metricasVentaService, never()).registrarVenta(any(), anyString());
    }
}
