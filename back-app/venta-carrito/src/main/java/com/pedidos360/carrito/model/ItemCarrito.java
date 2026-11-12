package com.pedidos360.carrito.model;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.time.Instant;
import java.time.LocalDateTime;

@Entity
@Table(name = "items_carrito")
public class ItemCarrito {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // Identificador del usuario autenticado (viene del claim del JWT, ej: "sub" o "oid")
    @NotBlank
    @Column(nullable = false, length = 120)
    private String usuarioId;

    @NotNull
    @Column(nullable = false)
    private Long productoId;

    @Column(length = 150)
    private String nombreProducto;

    @NotNull
    @Positive
    @Column(nullable = false)
    private Integer cantidad;

    @NotNull
    @Positive
    @Column(nullable = false)
    private Double precioUnitario;

    @Column(nullable = false)
    private Double total;

    @Column(nullable = false)
    private LocalDateTime fechaCreacion;

    // --- Sincronizados desde Kafka (topico pedidos360.inventario.actualizado) ---

    // false si el producto se agoto o fue eliminado del catalogo: no se puede confirmar el pedido.
    // null en items creados antes de la EP3 = disponible.
    private Boolean disponible = Boolean.TRUE;

    // fechaEvento del ultimo cambio de inventario aplicado; sirve para descartar eventos atrasados
    private Instant actualizadoDesdeInventario;

    public ItemCarrito() {
    }

    public ItemCarrito(String usuarioId, Long productoId, String nombreProducto, Integer cantidad, Double precioUnitario) {
        this.usuarioId = usuarioId;
        this.productoId = productoId;
        this.nombreProducto = nombreProducto;
        this.cantidad = cantidad;
        this.precioUnitario = precioUnitario;
        this.total = cantidad * precioUnitario;
        this.fechaCreacion = LocalDateTime.now();
    }

    @PrePersist
    public void calcularTotalYFecha() {
        if (this.total == null && this.cantidad != null && this.precioUnitario != null) {
            this.total = this.cantidad * this.precioUnitario;
        }
        if (this.fechaCreacion == null) {
            this.fechaCreacion = LocalDateTime.now();
        }
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getUsuarioId() {
        return usuarioId;
    }

    public void setUsuarioId(String usuarioId) {
        this.usuarioId = usuarioId;
    }

    public Long getProductoId() {
        return productoId;
    }

    public void setProductoId(Long productoId) {
        this.productoId = productoId;
    }

    public String getNombreProducto() {
        return nombreProducto;
    }

    public void setNombreProducto(String nombreProducto) {
        this.nombreProducto = nombreProducto;
    }

    public Integer getCantidad() {
        return cantidad;
    }

    public void setCantidad(Integer cantidad) {
        this.cantidad = cantidad;
    }

    public Double getPrecioUnitario() {
        return precioUnitario;
    }

    public void setPrecioUnitario(Double precioUnitario) {
        this.precioUnitario = precioUnitario;
    }

    public Double getTotal() {
        return total;
    }

    public void setTotal(Double total) {
        this.total = total;
    }

    public LocalDateTime getFechaCreacion() {
        return fechaCreacion;
    }

    public void setFechaCreacion(LocalDateTime fechaCreacion) {
        this.fechaCreacion = fechaCreacion;
    }

    public Boolean getDisponible() {
        return disponible == null || disponible;
    }

    public void setDisponible(Boolean disponible) {
        this.disponible = disponible;
    }

    public Instant getActualizadoDesdeInventario() {
        return actualizadoDesdeInventario;
    }

    public void setActualizadoDesdeInventario(Instant actualizadoDesdeInventario) {
        this.actualizadoDesdeInventario = actualizadoDesdeInventario;
    }

    /** Aplica el precio y la disponibilidad vigentes del catalogo y recalcula el total. */
    public void aplicarInventario(double precioVigente, boolean disponibleVigente, Instant fechaEvento) {
        this.precioUnitario = precioVigente;
        this.total = this.cantidad * precioVigente;
        this.disponible = disponibleVigente;
        this.actualizadoDesdeInventario = fechaEvento;
    }
}
