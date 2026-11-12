package com.pedidos360.carrito.repository;

import com.pedidos360.carrito.model.ItemCarrito;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface ItemCarritoRepository extends JpaRepository<ItemCarrito, Long> {

    List<ItemCarrito> findByUsuarioId(String usuarioId);

    /** Items de todos los carritos que contienen un producto (para aplicar cambios de inventario). */
    List<ItemCarrito> findByProductoId(Long productoId);

    void deleteByUsuarioIdAndId(String usuarioId, Long id);
}
