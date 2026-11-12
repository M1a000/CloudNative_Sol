package com.pedidos360.catalogo.dto.kafka;

/** Que provoco el cambio de inventario publicado en pedidos360.inventario.actualizado. */
public enum TipoEventoInventario {
    /** Alta de un producto via POST /api/productos. */
    PRODUCTO_CREADO,
    /** Cambio de precio, stock u otros datos via PUT /api/productos/{id}. */
    PRODUCTO_ACTUALIZADO,
    /** Descuento de stock por un pedido recibido desde RabbitMQ (pedido.creado). */
    STOCK_DESCONTADO
}
