package com.pedidos360.catalogo.service;

import com.pedidos360.catalogo.dto.kafka.VentaConfirmadaEvent;
import com.pedidos360.catalogo.exception.ReglaNegocioException;
import com.pedidos360.catalogo.model.EventoProcesado;
import com.pedidos360.catalogo.model.MetricaVentaProducto;
import com.pedidos360.catalogo.repository.EventoProcesadoRepository;
import com.pedidos360.catalogo.repository.MetricaVentaProductoRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Logica de negocio del caso de uso 1 (analitica de ventas), sin Kafka. */
@ExtendWith(MockitoExtension.class)
class MetricasVentaServiceTest {

    private static final String TOPICO = "pedidos360.ventas.confirmadas";

    @Mock
    private MetricaVentaProductoRepository metricaRepository;

    @Mock
    private EventoProcesadoRepository eventoProcesadoRepository;

    @InjectMocks
    private MetricasVentaService service;

    static VentaConfirmadaEvent venta(String eventoId, List<VentaConfirmadaEvent.Item> items) {
        int unidades = items.stream().mapToInt(VentaConfirmadaEvent.Item::cantidad).sum();
        double total = items.stream().mapToDouble(VentaConfirmadaEvent.Item::subtotal).sum();
        return new VentaConfirmadaEvent(eventoId, VentaConfirmadaEvent.TIPO, 1, Instant.parse("2026-11-13T18:00:00Z"),
                "venta-carrito", "PED-1", "usuario-1", items, unidades, total, "CLP");
    }

    static VentaConfirmadaEvent.Item item(long productoId, int cantidad, double precio) {
        return new VentaConfirmadaEvent.Item(productoId, "Producto " + productoId, cantidad, precio, cantidad * precio);
    }

    @Test
    void registraLaVentaEnLasMetricasDeCadaProductoYGuardaElEvento() {
        when(eventoProcesadoRepository.existsById("ev-1")).thenReturn(false);
        when(metricaRepository.findById(any())).thenReturn(Optional.empty());

        boolean aplicada = service.registrarVenta(venta("ev-1", List.of(item(1, 2, 5990), item(4, 1, 2990))), TOPICO);

        assertThat(aplicada).isTrue();
        ArgumentCaptor<MetricaVentaProducto> guardadas = ArgumentCaptor.forClass(MetricaVentaProducto.class);
        verify(metricaRepository, times(2)).save(guardadas.capture());
        MetricaVentaProducto clasica = guardadas.getAllValues().get(0);
        assertThat(clasica.getProductoId()).isEqualTo(1L);
        assertThat(clasica.getUnidadesVendidas()).isEqualTo(2L);
        assertThat(clasica.getMontoVendido()).isEqualTo(11980.0);
        assertThat(clasica.getCantidadPedidos()).isEqualTo(1);
        verify(eventoProcesadoRepository).save(any(EventoProcesado.class));
    }

    @Test
    void unEventoRepetidoSeIgnoraPorIdempotencia() {
        when(eventoProcesadoRepository.existsById("ev-1")).thenReturn(true);

        boolean aplicada = service.registrarVenta(venta("ev-1", List.of(item(1, 2, 5990))), TOPICO);

        assertThat(aplicada).isFalse();
        verify(metricaRepository, never()).save(any());
        verify(eventoProcesadoRepository, never()).save(any());
    }

    @Test
    void sumaALasMetricasExistentesYAgrupaLineasDelMismoProducto() {
        MetricaVentaProducto existente = new MetricaVentaProducto(1L);
        existente.registrarVenta("Producto 1", 3, 17970, Instant.parse("2026-11-12T10:00:00Z"));
        when(eventoProcesadoRepository.existsById("ev-2")).thenReturn(false);
        when(metricaRepository.findById(1L)).thenReturn(Optional.of(existente));

        service.registrarVenta(venta("ev-2", List.of(item(1, 1, 5990), item(1, 2, 5990))), TOPICO);

        // Las 2 lineas del mismo producto cuentan como 1 pedido con 3 unidades
        verify(metricaRepository, times(1)).save(existente);
        assertThat(existente.getUnidadesVendidas()).isEqualTo(6L);
        assertThat(existente.getCantidadPedidos()).isEqualTo(2);
    }

    @Test
    void rechazaUnaVentaConTotalIncoherenteSinTocarLaBaseDeDatos() {
        VentaConfirmadaEvent correcta = venta("ev-3", List.of(item(1, 2, 5990)));
        VentaConfirmadaEvent totalAlterado = new VentaConfirmadaEvent(correcta.eventoId(), correcta.tipoEvento(),
                correcta.version(), correcta.fechaEvento(), correcta.origen(), correcta.pedidoId(),
                correcta.usuarioId(), correcta.items(), correcta.cantidadUnidades(), 1.0, correcta.moneda());

        assertThatThrownBy(() -> service.registrarVenta(totalAlterado, TOPICO))
                .isInstanceOf(ReglaNegocioException.class)
                .hasMessageContaining("total incorrecto");
        verify(metricaRepository, never()).save(any());
    }

    @Test
    void rechazaTipoDeEventoYVersionDeEsquemaNoSoportados() {
        VentaConfirmadaEvent base = venta("ev-4", List.of(item(1, 1, 5990)));
        VentaConfirmadaEvent otroTipo = new VentaConfirmadaEvent(base.eventoId(), "OTRO", 1, base.fechaEvento(),
                base.origen(), base.pedidoId(), base.usuarioId(), base.items(), base.cantidadUnidades(),
                base.total(), base.moneda());
        VentaConfirmadaEvent version2 = new VentaConfirmadaEvent(base.eventoId(), base.tipoEvento(), 2,
                base.fechaEvento(), base.origen(), base.pedidoId(), base.usuarioId(), base.items(),
                base.cantidadUnidades(), base.total(), base.moneda());

        assertThatThrownBy(() -> service.registrarVenta(otroTipo, TOPICO)).isInstanceOf(ReglaNegocioException.class);
        assertThatThrownBy(() -> service.registrarVenta(version2, TOPICO)).isInstanceOf(ReglaNegocioException.class);
    }

    @Test
    void elRankingLimitaElTopEntre1Y50() {
        service.masVendidos(500);
        service.masVendidos(0);

        verify(metricaRepository).findAllByOrderByUnidadesVendidasDescMontoVendidoDesc(
                argThat((Pageable p) -> p.getPageSize() == 50));
        verify(metricaRepository).findAllByOrderByUnidadesVendidasDescMontoVendidoDesc(
                argThat((Pageable p) -> p.getPageSize() == 1));
    }
}
