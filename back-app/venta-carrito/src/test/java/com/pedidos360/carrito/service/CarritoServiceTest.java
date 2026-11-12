package com.pedidos360.carrito.service;

import com.pedidos360.carrito.dto.PedidoCreadoEvent;
import com.pedidos360.carrito.exception.CarritoVacioException;
import com.pedidos360.carrito.exception.ProductoSinStockException;
import com.pedidos360.carrito.model.ItemCarrito;
import com.pedidos360.carrito.repository.ItemCarritoRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CarritoServiceTest {

    @Mock
    private ItemCarritoRepository itemCarritoRepository;

    @Mock
    private PedidoEventPublisher pedidoEventPublisher;

    @Mock
    private VentaEventPublisher ventaEventPublisher;

    @InjectMocks
    private CarritoService carritoService;

    @Test
    void confirmarPedidoPublicaElEventoYVaciaElCarrito() {
        ItemCarrito hamburguesa = new ItemCarrito("user-123", 1L, "Hamburguesa Clasica", 2, 4990.0);
        ItemCarrito bebida = new ItemCarrito("user-123", 3L, "Bebida", 1, 1500.0);
        List<ItemCarrito> items = List.of(hamburguesa, bebida);
        when(itemCarritoRepository.findByUsuarioId("user-123")).thenReturn(items);

        PedidoCreadoEvent evento = carritoService.confirmarPedido("user-123");

        assertThat(evento.pedidoId()).isNotBlank();
        assertThat(evento.usuarioId()).isEqualTo("user-123");
        assertThat(evento.items()).hasSize(2);
        assertThat(evento.total()).isEqualTo(11480.0);
        verify(itemCarritoRepository).deleteAll(items);
        verify(pedidoEventPublisher).publicarPedidoCreado(evento);
        verify(ventaEventPublisher).publicarVentaConfirmada(argThat(venta ->
                venta.pedidoId().equals(evento.pedidoId())
                        && venta.cantidadUnidades() == 3
                        && venta.total() == 11480.0));
    }

    @Test
    void confirmarCarritoVacioLanzaExcepcionYNoPublicaNada() {
        when(itemCarritoRepository.findByUsuarioId("user-123")).thenReturn(List.of());

        assertThatThrownBy(() -> carritoService.confirmarPedido("user-123"))
                .isInstanceOf(CarritoVacioException.class);

        verifyNoInteractions(pedidoEventPublisher);
    }

    @Test
    void confirmarConProductoAgotadoLanzaExcepcionYNoPublicaNada() {
        ItemCarrito hamburguesa = new ItemCarrito("user-123", 1L, "Hamburguesa Clasica", 2, 4990.0);
        ItemCarrito bebida = new ItemCarrito("user-123", 3L, "Bebida", 1, 1500.0);
        bebida.setDisponible(false); // marcado sin stock por el consumidor Kafka de inventario
        when(itemCarritoRepository.findByUsuarioId("user-123")).thenReturn(List.of(hamburguesa, bebida));

        assertThatThrownBy(() -> carritoService.confirmarPedido("user-123"))
                .isInstanceOf(ProductoSinStockException.class)
                .hasMessageContaining("Bebida");

        verifyNoInteractions(pedidoEventPublisher, ventaEventPublisher);
        verify(itemCarritoRepository, never()).deleteAll(any());
    }
}
