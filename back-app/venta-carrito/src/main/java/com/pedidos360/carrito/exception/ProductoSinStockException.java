package com.pedidos360.carrito.exception;

import java.util.List;

/**
 * Se intento confirmar un carrito con productos agotados o eliminados del catalogo (HTTP 409).
 * La disponibilidad la mantiene al dia el consumidor Kafka de pedidos360.inventario.actualizado.
 */
public class ProductoSinStockException extends RuntimeException {

    private final List<String> productos;

    public ProductoSinStockException(List<String> productos) {
        super("No se puede confirmar el pedido: sin stock " + productos
                + ". Quite esos productos del carrito e intente nuevamente.");
        this.productos = List.copyOf(productos);
    }

    public List<String> getProductos() {
        return productos;
    }
}
