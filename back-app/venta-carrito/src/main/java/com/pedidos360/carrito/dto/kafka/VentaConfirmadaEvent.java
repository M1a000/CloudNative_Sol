package com.pedidos360.carrito.dto.kafka;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.pedidos360.carrito.dto.PedidoCreadoEvent;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;

import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;

/**
 * Evento del topico pedidos360.ventas.confirmadas (lo publica este servicio).
 * Contrato JSON definido en docs/kafka/modelo-mensajes.md.
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
    public static final int VERSION = 1;
    public static final String ORIGEN = "venta-carrito";
    public static final String MONEDA = "CLP";

    public record Item(
            @NotNull Long productoId,
            String nombreProducto,
            @NotNull @Positive Integer cantidad,
            @NotNull @PositiveOrZero Double precioUnitario,
            @NotNull Double subtotal) {
    }

    /**
     * Arma el evento de analitica a partir del pedido que viaja por RabbitMQ:
     * mismo pedidoId, para poder trazar el pedido en ambos brokers.
     */
    public static VentaConfirmadaEvent desde(PedidoCreadoEvent pedido) {
        List<Item> items = pedido.items().stream()
                .map(i -> new Item(i.productoId(), i.nombreProducto(), i.cantidad(), i.precioUnitario(),
                        i.cantidad() * i.precioUnitario()))
                .toList();
        int unidades = items.stream().mapToInt(Item::cantidad).sum();
        Instant fecha = pedido.fechaCreacion() == null
                ? Instant.now()
                : pedido.fechaCreacion().atZone(ZoneId.systemDefault()).toInstant();

        return new VentaConfirmadaEvent(UUID.randomUUID().toString(), TIPO, VERSION, fecha, ORIGEN,
                pedido.pedidoId(), pedido.usuarioId(), items, unidades, pedido.total(), MONEDA);
    }
}
