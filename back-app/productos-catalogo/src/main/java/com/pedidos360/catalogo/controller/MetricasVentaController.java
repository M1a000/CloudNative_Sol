package com.pedidos360.catalogo.controller;

import com.pedidos360.catalogo.model.MetricaVentaProducto;
import com.pedidos360.catalogo.service.MetricasVentaService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Analitica de ventas alimentada por Kafka (topico pedidos360.ventas.confirmadas).
 * Protegido con JWT como el resto de /api/productos.
 */
@RestController
@RequestMapping("/api/productos")
public class MetricasVentaController {

    private final MetricasVentaService metricasVentaService;

    public MetricasVentaController(MetricasVentaService metricasVentaService) {
        this.metricasVentaService = metricasVentaService;
    }

    /** Ranking de productos mas vendidos. top: entre 1 y 50 (por defecto 5). */
    @GetMapping("/mas-vendidos")
    public ResponseEntity<List<MetricaVentaProducto>> masVendidos(@RequestParam(defaultValue = "5") int top) {
        return ResponseEntity.ok(metricasVentaService.masVendidos(top));
    }
}
