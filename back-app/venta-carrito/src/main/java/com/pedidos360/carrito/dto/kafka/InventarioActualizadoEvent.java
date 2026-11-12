package com.pedidos360.carrito.dto.kafka;

import com.fasterxml.jackson.annotation.JsonFormat;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;

import java.time.Instant;

/**
 * Evento del topico pedidos360.inventario.actualizado (lo publica productos-catalogo).
 * Copia local del contrato definido en docs/kafka/modelo-mensajes.md.
 * Un mensaje con valor null (tombstone) significa "producto eliminado".
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

    public static final int VERSION_SOPORTADA = 1;
}
