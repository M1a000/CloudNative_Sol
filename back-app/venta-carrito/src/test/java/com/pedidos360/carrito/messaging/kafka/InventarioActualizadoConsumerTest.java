package com.pedidos360.carrito.messaging.kafka;

import com.pedidos360.carrito.dto.kafka.InventarioActualizadoEvent;
import com.pedidos360.carrito.dto.kafka.TipoEventoInventario;
import com.pedidos360.carrito.messaging.comun.MensajeInvalidoException;
import com.pedidos360.carrito.messaging.comun.ValidadorMensajes;
import com.pedidos360.carrito.messaging.kafka.inventario.InventarioActualizadoConsumer;
import com.pedidos360.carrito.service.SincronizacionInventarioService;
import jakarta.validation.Validation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** El consumidor valida (incluida la key) y delega; los invalidos lanzan MensajeInvalidoException -> DLT. */
class InventarioActualizadoConsumerTest {

    private static final String TOPICO = "pedidos360.inventario.actualizado";

    private SincronizacionInventarioService sincronizacionService;
    private InventarioActualizadoConsumer consumer;

    @BeforeEach
    void setUp() {
        sincronizacionService = mock(SincronizacionInventarioService.class);
        ValidadorMensajes validador = new ValidadorMensajes(Validation.buildDefaultValidatorFactory().getValidator());
        consumer = new InventarioActualizadoConsumer(sincronizacionService, validador);
    }

    private static InventarioActualizadoEvent stockAgotado() {
        return new InventarioActualizadoEvent("ev-1", TipoEventoInventario.STOCK_DESCONTADO, 1, Instant.now(),
                "productos-catalogo", 3L, "Hamburguesa Doble", "Hamburguesas", 8990.0, 6, 0, false, "PED-1");
    }

    private static ConsumerRecord<String, InventarioActualizadoEvent> registro(String key, InventarioActualizadoEvent v) {
        return new ConsumerRecord<>(TOPICO, 2, 0L, key, v);
    }

    @Test
    void unEventoValidoSeAplicaALosCarritos() {
        InventarioActualizadoEvent evento = stockAgotado();
        when(sincronizacionService.aplicar(evento)).thenReturn(new SincronizacionInventarioService.Resultado(1, 0));

        consumer.alRecibirCambioInventario(registro("3", evento));

        verify(sincronizacionService).aplicar(evento);
    }

    @Test
    void unaKeyDistintaDelProductoIdEsInvalida() {
        assertThatThrownBy(() -> consumer.alRecibirCambioInventario(registro("99", stockAgotado())))
                .isInstanceOf(MensajeInvalidoException.class)
                .hasMessageContaining("no coincide");
        verify(sincronizacionService, never()).aplicar(any());
    }

    @Test
    void unEventoSinCamposObligatoriosEsInvalido() {
        InventarioActualizadoEvent sinNombre = new InventarioActualizadoEvent("ev-2",
                TipoEventoInventario.STOCK_DESCONTADO, 1, Instant.now(), "productos-catalogo", 3L, "",
                null, 8990.0, 6, 0, false, null);

        assertThatThrownBy(() -> consumer.alRecibirCambioInventario(registro("3", sinNombre)))
                .isInstanceOf(MensajeInvalidoException.class)
                .hasMessageContaining("nombre");
        verify(sincronizacionService, never()).aplicar(any());
    }

    @Test
    void unTombstoneMarcaElProductoComoEliminado() {
        consumer.alRecibirCambioInventario(registro("7", null));

        verify(sincronizacionService).marcarEliminado(7L);
    }

    @Test
    void unTombstoneConKeyNoNumericaEsInvalido() {
        assertThatThrownBy(() -> consumer.alRecibirCambioInventario(registro("abc", null)))
                .isInstanceOf(MensajeInvalidoException.class);
    }
}
