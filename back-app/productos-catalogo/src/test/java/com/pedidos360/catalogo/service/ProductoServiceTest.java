package com.pedidos360.catalogo.service;

import com.pedidos360.catalogo.dto.kafka.TipoEventoInventario;
import com.pedidos360.catalogo.exception.ProductoNoEncontradoException;
import com.pedidos360.catalogo.exception.StockInsuficienteException;
import com.pedidos360.catalogo.model.Producto;
import com.pedidos360.catalogo.repository.ProductoRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProductoServiceTest {

    @Mock
    private ProductoRepository productoRepository;

    @Mock
    private InventarioEventPublisher inventarioEventPublisher;

    @InjectMocks
    private ProductoService productoService;

    @Test
    void descontarStockRestaLaCantidadPedida() {
        Producto hamburguesa = new Producto("Hamburguesa Clasica", "Con queso", 4990.0, 10, "Hamburguesas");
        hamburguesa.setId(1L);
        when(productoRepository.findById(1L)).thenReturn(Optional.of(hamburguesa));

        productoService.descontarStock(Map.of(1L, 3));

        assertThat(hamburguesa.getStock()).isEqualTo(7);
        verify(productoRepository).save(hamburguesa);
        verify(inventarioEventPublisher).publicar(argThat(evento ->
                evento.tipoEvento() == TipoEventoInventario.STOCK_DESCONTADO
                        && evento.stockAnterior() == 10 && evento.stockActual() == 7));
    }

    @Test
    void descontarStockSinStockSuficienteLanzaExcepcionYNoGuarda() {
        Producto bebida = new Producto("Bebida", "Lata", 1500.0, 2, "Bebidas");
        bebida.setId(3L);
        when(productoRepository.findById(3L)).thenReturn(Optional.of(bebida));

        assertThatThrownBy(() -> productoService.descontarStock(Map.of(3L, 5)))
                .isInstanceOf(StockInsuficienteException.class)
                .hasMessageContaining("disponible 2");

        assertThat(bebida.getStock()).isEqualTo(2);
        verify(productoRepository, never()).save(any(Producto.class));
    }

    @Test
    void descontarStockDeProductoInexistenteLanzaExcepcion() {
        when(productoRepository.findById(99L)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> productoService.descontarStock(Map.of(99L, 1)))
                .isInstanceOf(ProductoNoEncontradoException.class);
    }
}
