package com.pedidos360.catalogo.service;

import com.pedidos360.catalogo.dto.kafka.VentaConfirmadaEvent;
import com.pedidos360.catalogo.exception.ReglaNegocioException;
import com.pedidos360.catalogo.model.EventoProcesado;
import com.pedidos360.catalogo.model.MetricaVentaProducto;
import com.pedidos360.catalogo.repository.EventoProcesadoRepository;
import com.pedidos360.catalogo.repository.MetricaVentaProductoRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Logica de negocio de la analitica de ventas. No sabe nada de Kafka: el consumidor
 * solo le entrega el evento ya deserializado y validado.
 */
@Service
public class MetricasVentaService {

    /** Tolerancia para comparar montos (redondeos de decimales). */
    private static final double TOLERANCIA = 1.0;

    private final MetricaVentaProductoRepository metricaRepository;
    private final EventoProcesadoRepository eventoProcesadoRepository;

    public MetricasVentaService(MetricaVentaProductoRepository metricaRepository,
                                EventoProcesadoRepository eventoProcesadoRepository) {
        this.metricaRepository = metricaRepository;
        this.eventoProcesadoRepository = eventoProcesadoRepository;
    }

    /**
     * Suma la venta a las metricas de cada producto, en una sola transaccion e idempotente.
     *
     * @param topico topico de origen, queda en el registro de idempotencia
     * @return true si se aplico; false si el evento ya habia sido procesado (duplicado)
     * @throws ReglaNegocioException si el evento es incoherente (no se reintenta: va al DLT)
     */
    @Transactional
    public boolean registrarVenta(VentaConfirmadaEvent venta, String topico) {
        validarCoherencia(venta);

        if (eventoProcesadoRepository.existsById(venta.eventoId())) {
            return false;
        }

        Map<Long, VentaConfirmadaEvent.Item> porProducto = agruparPorProducto(venta.items());
        porProducto.forEach((productoId, item) -> {
            MetricaVentaProducto metrica = metricaRepository.findById(productoId)
                    .orElseGet(() -> new MetricaVentaProducto(productoId));
            metrica.registrarVenta(item.nombreProducto(), item.cantidad(), item.subtotal(), venta.fechaEvento());
            metricaRepository.save(metrica);
        });

        eventoProcesadoRepository.save(new EventoProcesado(venta.eventoId(), topico));
        return true;
    }

    /** Ranking de los productos mas vendidos (top entre 1 y 50). */
    @Transactional(readOnly = true)
    public List<MetricaVentaProducto> masVendidos(int top) {
        int limite = Math.max(1, Math.min(top, 50));
        return metricaRepository.findAllByOrderByUnidadesVendidasDescMontoVendidoDesc(PageRequest.of(0, limite));
    }

    /** Reglas que las anotaciones del DTO no pueden expresar (relaciones entre campos). */
    void validarCoherencia(VentaConfirmadaEvent venta) {
        if (!VentaConfirmadaEvent.TIPO.equals(venta.tipoEvento())) {
            throw new ReglaNegocioException("tipoEvento no soportado: " + venta.tipoEvento());
        }
        if (venta.version() != VentaConfirmadaEvent.VERSION_SOPORTADA) {
            throw new ReglaNegocioException("version de esquema no soportada: " + venta.version());
        }
        double sumaSubtotales = 0;
        int sumaUnidades = 0;
        for (VentaConfirmadaEvent.Item item : venta.items()) {
            double esperado = item.cantidad() * item.precioUnitario();
            if (Math.abs(esperado - item.subtotal()) > TOLERANCIA) {
                throw new ReglaNegocioException("subtotal incorrecto en producto " + item.productoId()
                        + ": esperado " + esperado + ", recibido " + item.subtotal());
            }
            sumaSubtotales += item.subtotal();
            sumaUnidades += item.cantidad();
        }
        if (Math.abs(sumaSubtotales - venta.total()) > TOLERANCIA) {
            throw new ReglaNegocioException("total incorrecto: suma de subtotales " + sumaSubtotales
                    + ", total recibido " + venta.total());
        }
        if (sumaUnidades != venta.cantidadUnidades()) {
            throw new ReglaNegocioException("cantidadUnidades incorrecta: esperado " + sumaUnidades
                    + ", recibido " + venta.cantidadUnidades());
        }
    }

    /** Si un producto aparece en varias lineas, se suman cantidades y subtotales. */
    private Map<Long, VentaConfirmadaEvent.Item> agruparPorProducto(List<VentaConfirmadaEvent.Item> items) {
        Map<Long, VentaConfirmadaEvent.Item> agrupado = new LinkedHashMap<>();
        for (VentaConfirmadaEvent.Item item : items) {
            agrupado.merge(item.productoId(), item, (a, b) -> new VentaConfirmadaEvent.Item(
                    a.productoId(), a.nombreProducto(), a.cantidad() + b.cantidad(),
                    a.precioUnitario(), a.subtotal() + b.subtotal()));
        }
        return agrupado;
    }
}
