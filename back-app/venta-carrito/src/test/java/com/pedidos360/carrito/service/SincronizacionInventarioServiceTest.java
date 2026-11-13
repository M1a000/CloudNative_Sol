package com.pedidos360.carrito.service;

import com.pedidos360.carrito.dto.kafka.InventarioActualizadoEvent;
import com.pedidos360.carrito.dto.kafka.TipoEventoInventario;
import com.pedidos360.carrito.messaging.comun.MensajeInvalidoException;
import com.pedidos360.carrito.model.ItemCarrito;
import com.pedidos360.carrito.repository.ItemCarritoRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Logica de negocio del caso de uso 2 (estado del inventario en los carritos), sin Kafka. */
@ExtendWith(MockitoExtension.class)
class SincronizacionInventarioServiceTest {

    private static final Instant T1 = Instant.parse("2026-11-13T18:00:00Z");
    private static final Instant T0 = Instant.parse("2026-11-13T17:00:00Z");

    @Mock
    private ItemCarritoRepository itemCarritoRepository;

    @InjectMocks
    private SincronizacionInventarioService service;

    static InventarioActualizadoEvent evento(int stock, double precio, Instant fecha) {
        return new InventarioActualizadoEvent("ev-1", TipoEventoInventario.STOCK_DESCONTADO, 1, fecha,
                "productos-catalogo", 3L, "Hamburguesa Doble", "Hamburguesas", precio, 6, stock, stock > 0, "PED-1");
    }

    private static ItemCarrito itemDoble(String usuario) {
        return new ItemCarrito(usuario, 3L, "Hamburguesa Doble", 2, 8990.0);
    }

    @Test
    void marcaSinStockTodosLosCarritosConElProductoAgotado() {
        ItemCarrito deAna = itemDoble("ana");
        ItemCarrito deLuis = itemDoble("luis");
        when(itemCarritoRepository.findByProductoId(3L)).thenReturn(List.of(deAna, deLuis));

        SincronizacionInventarioService.Resultado resultado = service.aplicar(evento(0, 8990, T1));

        assertThat(resultado.actualizados()).isEqualTo(2);
        assertThat(deAna.getDisponible()).isFalse();
        assertThat(deLuis.getDisponible()).isFalse();
        verify(itemCarritoRepository).save(deAna);
        verify(itemCarritoRepository).save(deLuis);
    }

    @Test
    void aplicaElPrecioVigenteYRecalculaElTotal() {
        ItemCarrito item = itemDoble("ana");
        when(itemCarritoRepository.findByProductoId(3L)).thenReturn(List.of(item));

        service.aplicar(evento(5, 9990, T1));

        assertThat(item.getPrecioUnitario()).isEqualTo(9990.0);
        assertThat(item.getTotal()).isEqualTo(19980.0);
        assertThat(item.getDisponible()).isTrue();
    }

    @Test
    void descartaUnEventoMasAntiguoQueElUltimoAplicado() {
        ItemCarrito item = itemDoble("ana");
        item.aplicarInventario(8990, false, T1);
        when(itemCarritoRepository.findByProductoId(3L)).thenReturn(List.of(item));

        SincronizacionInventarioService.Resultado resultado = service.aplicar(evento(5, 8990, T0));

        assertThat(resultado.descartados()).isEqualTo(1);
        assertThat(item.getDisponible()).isFalse();
        verify(itemCarritoRepository, never()).save(any());
    }

    @Test
    void rechazaUnEventoIncoherente() {
        InventarioActualizadoEvent incoherente = new InventarioActualizadoEvent("ev-2",
                TipoEventoInventario.PRODUCTO_ACTUALIZADO, 1, T1, "productos-catalogo", 3L, "Hamburguesa Doble",
                "Hamburguesas", 8990.0, 6, 0, true, null);

        assertThatThrownBy(() -> service.aplicar(incoherente))
                .isInstanceOf(MensajeInvalidoException.class)
                .hasMessageContaining("disponible");
        verify(itemCarritoRepository, never()).findByProductoId(any());
    }

    @Test
    void unProductoEliminadoQuedaSinStockEnLosCarritos() {
        ItemCarrito item = itemDoble("ana");
        when(itemCarritoRepository.findByProductoId(3L)).thenReturn(List.of(item));

        int afectados = service.marcarEliminado(3L);

        assertThat(afectados).isEqualTo(1);
        assertThat(item.getDisponible()).isFalse();
    }
}
