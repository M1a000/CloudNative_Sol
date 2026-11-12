package com.pedidos360.catalogo.dto.kafka;

import com.fasterxml.jackson.annotation.JsonFormat;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;

import java.time.Instant;
import java.util.List;

/**
 * Evento del topico pedidos360.ventas.confirmadas (lo publica venta-carrito).
 * Copia local del contrato definido en docs/kafka/modelo-mensajes.md: los servicios no
 * comparten clases Java, solo el formato JSON.
 */
public record VentaConfirmadaEvent(
        @NotBlank String eventoId,
        @NotBlank String tipoEvento,
        @NotNull Integer version,
        @NotNull @JsonFormat(shape = JsonFormat.Shape.STRING) Instant fechaEvento,
        @NotBlank String origen,
        @NotBlank String pedidoId,
        @NotBlank String usuarioId,
        @NotEmpty List<@Valid @NotNull Item> items,
        @NotNull @Positive Integer cantidadUnidades,
        @NotNull @PositiveOrZero Double total,
        @NotBlank String moneda) {

    public static final String TIPO = "VENTA_CONFIRMADA";
    public static final int VERSION_SOPORTADA = 1;

    public record Item(
            @NotNull Long productoId,
            String nombreProducto,
            @NotNull @Positive Integer cantidad,
            @NotNull @PositiveOrZero Double precioUnitario,
            @NotNull Double subtotal) {
    }
}
