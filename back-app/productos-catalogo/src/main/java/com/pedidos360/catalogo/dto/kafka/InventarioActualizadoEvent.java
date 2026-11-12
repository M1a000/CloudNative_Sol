package com.pedidos360.catalogo.dto.kafka;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.pedidos360.catalogo.model.Producto;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;

import java.time.Instant;
import java.util.UUID;

/**
 * Evento del topico pedidos360.inventario.actualizado (lo publica productos-catalogo).
 * Lleva siempre el ESTADO COMPLETO del producto: como el topico es compactado, el ultimo
 * mensaje de cada productoId debe bastar por si solo. La eliminacion se publica como tombstone.
 */
public record InventarioActualizadoEvent(
        @NotBlank String eventoId,
        @NotNull TipoEventoInventario tipoEvento,
        @NotNull Integer version,
        @NotNull @JsonFormat(shape = JsonFormat.Shape.STRING) Instant fechaEvento,
        @NotBlank String origen,
        @NotNull Long productoId,
        @NotBlank String nombre,
        String categoria,
        @NotNull @PositiveOrZero Double precio,
        @PositiveOrZero Integer stockAnterior,
        @NotNull @PositiveOrZero Integer stockActual,
        @NotNull Boolean disponible,
        String pedidoId) {

    public static final int VERSION = 1;
    public static final String ORIGEN = "productos-catalogo";

    /** Arma el evento a partir del estado actual del producto. */
    public static InventarioActualizadoEvent desde(Producto producto, TipoEventoInventario tipo,
                                                   Integer stockAnterior, String pedidoId) {
        return new InventarioActualizadoEvent(
                UUID.randomUUID().toString(),
                tipo,
                VERSION,
                Instant.now(),
                ORIGEN,
                producto.getId(),
                producto.getNombre(),
                producto.getCategoria(),
                producto.getPrecio(),
                stockAnterior,
                producto.getStock(),
                producto.getStock() > 0,
                pedidoId);
    }
}
