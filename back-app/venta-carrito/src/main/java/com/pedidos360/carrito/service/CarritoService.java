package com.pedidos360.carrito.service;

import com.pedidos360.carrito.dto.PedidoCreadoEvent;
import com.pedidos360.carrito.dto.kafka.VentaConfirmadaEvent;
import com.pedidos360.carrito.exception.CarritoVacioException;
import com.pedidos360.carrito.exception.ProductoSinStockException;
import com.pedidos360.carrito.model.ItemCarrito;
import com.pedidos360.carrito.repository.ItemCarritoRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class CarritoService {

    private final ItemCarritoRepository itemCarritoRepository;
    private final PedidoEventPublisher pedidoEventPublisher;
    private final VentaEventPublisher ventaEventPublisher;

    public CarritoService(ItemCarritoRepository itemCarritoRepository, PedidoEventPublisher pedidoEventPublisher,
                          VentaEventPublisher ventaEventPublisher) {
        this.itemCarritoRepository = itemCarritoRepository;
        this.pedidoEventPublisher = pedidoEventPublisher;
        this.ventaEventPublisher = ventaEventPublisher;
    }

    public List<ItemCarrito> listarPorUsuario(String usuarioId) {
        return itemCarritoRepository.findByUsuarioId(usuarioId);
    }

    public ItemCarrito agregar(ItemCarrito item) {
        return itemCarritoRepository.save(item);
    }

    public Optional<ItemCarrito> actualizarCantidad(Long id, ItemCarrito datos) {
        return itemCarritoRepository.findById(id).map(item -> {
            item.setCantidad(datos.getCantidad());
            item.setTotal(item.getCantidad() * item.getPrecioUnitario());
            return itemCarritoRepository.save(item);
        });
    }

    public boolean eliminar(Long id) {
        if (!itemCarritoRepository.existsById(id)) {
            return false;
        }
        itemCarritoRepository.deleteById(id);
        return true;
    }

    /**
     * Confirma el carrito del usuario como pedido:
     * 1. verifica que ningun producto este agotado (disponibilidad sincronizada desde Kafka),
     * 2. arma el evento con los productos del carrito y vacia el carrito,
     * 3. publica "pedido.creado" por RabbitMQ (stock y ticket se procesan de forma asincrona),
     * 4. tras el commit, publica la venta en Kafka para la analitica (pedidos360.ventas.confirmadas).
     * Si la publicacion en RabbitMQ falla, la transaccion se revierte y el carrito no se pierde.
     *
     * @throws CarritoVacioException      si el usuario no tiene productos en el carrito
     * @throws ProductoSinStockException  si algun producto del carrito esta agotado o fue eliminado
     */
    @Transactional
    public PedidoCreadoEvent confirmarPedido(String usuarioId) {
        List<ItemCarrito> items = itemCarritoRepository.findByUsuarioId(usuarioId);
        if (items.isEmpty()) {
            throw new CarritoVacioException(usuarioId);
        }

        List<String> sinStock = items.stream()
                .filter(item -> !item.getDisponible())
                .map(item -> item.getNombreProducto() != null ? item.getNombreProducto() : "producto " + item.getProductoId())
                .toList();
        if (!sinStock.isEmpty()) {
            throw new ProductoSinStockException(sinStock);
        }

        List<PedidoCreadoEvent.Item> lineas = items.stream()
                .map(item -> new PedidoCreadoEvent.Item(
                        item.getProductoId(), item.getNombreProducto(), item.getCantidad(), item.getPrecioUnitario()))
                .toList();

        double total = items.stream()
                .mapToDouble(item -> item.getCantidad() * item.getPrecioUnitario())
                .sum();

        PedidoCreadoEvent evento = new PedidoCreadoEvent(
                UUID.randomUUID().toString(), usuarioId, lineas, total, LocalDateTime.now());

        itemCarritoRepository.deleteAll(items);
        pedidoEventPublisher.publicarPedidoCreado(evento);
        ventaEventPublisher.publicarVentaConfirmada(VentaConfirmadaEvent.desde(evento));
        return evento;
    }
}
