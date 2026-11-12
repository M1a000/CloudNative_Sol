package com.pedidos360.catalogo.service;

import com.pedidos360.catalogo.dto.kafka.InventarioActualizadoEvent;
import com.pedidos360.catalogo.dto.kafka.TipoEventoInventario;
import com.pedidos360.catalogo.exception.ProductoNoEncontradoException;
import com.pedidos360.catalogo.exception.StockInsuficienteException;
import com.pedidos360.catalogo.model.Producto;
import com.pedidos360.catalogo.repository.ProductoRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Logica de negocio del catalogo. No sabe nada de RabbitMQ ni de Kafka:
 * <ul>
 *   <li>el consumidor RabbitMQ solo llama a {@link #descontarStock(Map, String)} como cualquier otro cliente;</li>
 *   <li>cada cambio de inventario se anuncia mediante la interfaz {@link InventarioEventPublisher}.</li>
 * </ul>
 */
@Service
public class ProductoService {

    private final ProductoRepository productoRepository;
    private final InventarioEventPublisher inventarioEventPublisher;

    public ProductoService(ProductoRepository productoRepository, InventarioEventPublisher inventarioEventPublisher) {
        this.productoRepository = productoRepository;
        this.inventarioEventPublisher = inventarioEventPublisher;
    }

    public List<Producto> listarTodos() {
        return productoRepository.findAll();
    }

    public Optional<Producto> buscarPorId(Long id) {
        return productoRepository.findById(id);
    }

    public List<Producto> listarPorCategoria(String categoria) {
        return productoRepository.findByCategoriaIgnoreCase(categoria);
    }

    public Producto crear(Producto producto) {
        Producto guardado = productoRepository.save(producto);
        inventarioEventPublisher.publicar(
                InventarioActualizadoEvent.desde(guardado, TipoEventoInventario.PRODUCTO_CREADO, null, null));
        return guardado;
    }

    public Optional<Producto> actualizar(Long id, Producto datos) {
        return productoRepository.findById(id).map(producto -> {
            Integer stockAnterior = producto.getStock();
            producto.setNombre(datos.getNombre());
            producto.setDescripcion(datos.getDescripcion());
            producto.setPrecio(datos.getPrecio());
            producto.setStock(datos.getStock());
            producto.setCategoria(datos.getCategoria());
            Producto guardado = productoRepository.save(producto);
            inventarioEventPublisher.publicar(InventarioActualizadoEvent.desde(
                    guardado, TipoEventoInventario.PRODUCTO_ACTUALIZADO, stockAnterior, null));
            return guardado;
        });
    }

    public boolean eliminar(Long id) {
        if (!productoRepository.existsById(id)) {
            return false;
        }
        productoRepository.deleteById(id);
        inventarioEventPublisher.publicarEliminacion(id);
        return true;
    }

    /** Version sin pedido asociado (compatibilidad): equivale a {@code descontarStock(cantidades, null)}. */
    public void descontarStock(Map<Long, Integer> cantidadesPorProducto) {
        descontarStock(cantidadesPorProducto, null);
    }

    /**
     * Descuenta el stock de todos los productos de un pedido en una sola transaccion:
     * si un producto no existe o no alcanza el stock, no se descuenta ninguno.
     * Tras el commit se publica un evento STOCK_DESCONTADO por producto (Kafka).
     *
     * @param cantidadesPorProducto id del producto -> cantidad a descontar
     * @param pedidoId              pedido que origina el descuento (viaja en el evento para trazabilidad)
     * @throws ProductoNoEncontradoException si algun producto no existe
     * @throws StockInsuficienteException    si algun producto no tiene stock suficiente
     */
    @Transactional
    public void descontarStock(Map<Long, Integer> cantidadesPorProducto, String pedidoId) {
        cantidadesPorProducto.forEach((productoId, cantidad) -> {
            Producto producto = productoRepository.findById(productoId)
                    .orElseThrow(() -> new ProductoNoEncontradoException(productoId));

            if (producto.getStock() < cantidad) {
                throw new StockInsuficienteException(productoId, producto.getStock(), cantidad);
            }

            int stockAnterior = producto.getStock();
            producto.setStock(stockAnterior - cantidad);
            productoRepository.save(producto);
            // Se encola para despues del commit: si otro producto del pedido falla, no se publica nada
            inventarioEventPublisher.publicar(InventarioActualizadoEvent.desde(
                    producto, TipoEventoInventario.STOCK_DESCONTADO, stockAnterior, pedidoId));
        });
    }
}
