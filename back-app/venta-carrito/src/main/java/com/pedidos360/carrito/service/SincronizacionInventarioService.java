package com.pedidos360.carrito.service;

import com.pedidos360.carrito.dto.kafka.InventarioActualizadoEvent;
import com.pedidos360.carrito.messaging.comun.MensajeInvalidoException;
import com.pedidos360.carrito.model.ItemCarrito;
import com.pedidos360.carrito.repository.ItemCarritoRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * Aplica a los carritos los cambios de inventario publicados por productos-catalogo.
 * No sabe nada de Kafka: el consumidor le entrega el evento ya validado.
 *
 * Es idempotente: aplicar dos veces el mismo estado deja el carrito igual, y los eventos
 * mas antiguos que el ultimo aplicado se descartan (por si llegan fuera de orden tras un rebalanceo).
 */
@Service
public class SincronizacionInventarioService {

    /** Resultado de aplicar un evento: items actualizados y descartados por estar atrasados. */
    public record Resultado(int actualizados, int descartados) {
    }

    private final ItemCarritoRepository itemCarritoRepository;

    public SincronizacionInventarioService(ItemCarritoRepository itemCarritoRepository) {
        this.itemCarritoRepository = itemCarritoRepository;
    }

    /**
     * Actualiza precio, total y disponibilidad de todos los items en carrito del producto.
     *
     * @throws MensajeInvalidoException si el evento es incoherente (no se reintenta: va al DLT)
     */
    @Transactional
    public Resultado aplicar(InventarioActualizadoEvent evento) {
        validarCoherencia(evento);

        int actualizados = 0;
        int descartados = 0;
        for (ItemCarrito item : itemCarritoRepository.findByProductoId(evento.productoId())) {
            if (esAtrasado(item, evento.fechaEvento())) {
                descartados++;
                continue;
            }
            item.aplicarInventario(evento.precio(), evento.disponible(), evento.fechaEvento());
            itemCarritoRepository.save(item);
            actualizados++;
        }
        return new Resultado(actualizados, descartados);
    }

    /** Producto eliminado del catalogo (tombstone): sus items quedan como no disponibles. */
    @Transactional
    public int marcarEliminado(Long productoId) {
        List<ItemCarrito> items = itemCarritoRepository.findByProductoId(productoId);
        Instant ahora = Instant.now();
        for (ItemCarrito item : items) {
            item.setDisponible(false);
            item.setActualizadoDesdeInventario(ahora);
            itemCarritoRepository.save(item);
        }
        return items.size();
    }

    void validarCoherencia(InventarioActualizadoEvent evento) {
        if (evento.version() != InventarioActualizadoEvent.VERSION_SOPORTADA) {
            throw new MensajeInvalidoException("version de esquema no soportada: " + evento.version());
        }
        if (evento.disponible() != (evento.stockActual() > 0)) {
            throw new MensajeInvalidoException("disponible=" + evento.disponible()
                    + " no coincide con stockActual=" + evento.stockActual());
        }
        if (evento.precio() <= 0) {
            throw new MensajeInvalidoException("precio debe ser mayor a 0 para aplicarlo a un carrito");
        }
    }

    private boolean esAtrasado(ItemCarrito item, Instant fechaEvento) {
        return item.getActualizadoDesdeInventario() != null
                && fechaEvento.isBefore(item.getActualizadoDesdeInventario());
    }
}
