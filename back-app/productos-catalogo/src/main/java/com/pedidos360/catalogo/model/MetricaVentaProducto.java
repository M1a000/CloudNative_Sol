package com.pedidos360.catalogo.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.time.Instant;

/**
 * Ventas acumuladas de un producto, calculadas a partir del topico pedidos360.ventas.confirmadas.
 * Es una proyeccion de lectura: se puede reconstruir desde cero reseteando el offset del grupo
 * catalogo-metricas-ventas (mientras los eventos sigan dentro de la retencion de 7 dias).
 *
 * {@code @Version}: el grupo consume con 3 hilos; si dos hilos actualizan el mismo producto a la vez,
 * uno falla con bloqueo optimista y el manejador de errores lo reintenta (error recuperable).
 */
@Entity
@Table(name = "metricas_venta_producto")
public class MetricaVentaProducto {

    @Id
    private Long productoId;

    @Column(length = 150)
    private String nombreProducto;

    @Column(nullable = false)
    private Long unidadesVendidas = 0L;

    @Column(nullable = false)
    private Double montoVendido = 0.0;

    @Column(nullable = false)
    private Integer cantidadPedidos = 0;

    private Instant ultimaVenta;

    @Version
    private Long version;

    protected MetricaVentaProducto() {
    }

    public MetricaVentaProducto(Long productoId) {
        this.productoId = productoId;
    }

    /** Suma una venta del producto a los acumulados. */
    public void registrarVenta(String nombreProducto, int unidades, double monto, Instant fecha) {
        if (nombreProducto != null && !nombreProducto.isBlank()) {
            this.nombreProducto = nombreProducto;
        }
        this.unidadesVendidas += unidades;
        this.montoVendido += monto;
        this.cantidadPedidos += 1;
        if (ultimaVenta == null || fecha.isAfter(ultimaVenta)) {
            this.ultimaVenta = fecha;
        }
    }

    public Long getProductoId() {
        return productoId;
    }

    public String getNombreProducto() {
        return nombreProducto;
    }

    public Long getUnidadesVendidas() {
        return unidadesVendidas;
    }

    public Double getMontoVendido() {
        return montoVendido;
    }

    public Integer getCantidadPedidos() {
        return cantidadPedidos;
    }

    public Instant getUltimaVenta() {
        return ultimaVenta;
    }
}
