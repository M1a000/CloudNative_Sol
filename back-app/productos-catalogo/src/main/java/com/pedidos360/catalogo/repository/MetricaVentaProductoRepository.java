package com.pedidos360.catalogo.repository;

import com.pedidos360.catalogo.model.MetricaVentaProducto;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface MetricaVentaProductoRepository extends JpaRepository<MetricaVentaProducto, Long> {

    /** Ranking de productos: mas unidades vendidas primero; a igualdad, mayor monto. */
    List<MetricaVentaProducto> findAllByOrderByUnidadesVendidasDescMontoVendidoDesc(Pageable pagina);
}
