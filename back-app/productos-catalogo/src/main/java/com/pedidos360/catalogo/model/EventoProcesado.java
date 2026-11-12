package com.pedidos360.catalogo.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Registro de idempotencia: eventoId de cada evento Kafka ya aplicado.
 * Kafka garantiza entrega "al menos una vez"; si un evento llega repetido (reintento del
 * productor, rebalanceo del grupo antes de confirmar el offset), no se vuelve a sumar.
 */
@Entity
@Table(name = "eventos_procesados")
public class EventoProcesado {

    @Id
    @Column(length = 36)
    private String eventoId;

    @Column(nullable = false, length = 120)
    private String topico;

    @Column(nullable = false)
    private Instant fechaProcesado;

    protected EventoProcesado() {
    }

    public EventoProcesado(String eventoId, String topico) {
        this.eventoId = eventoId;
        this.topico = topico;
        this.fechaProcesado = Instant.now();
    }

    public String getEventoId() {
        return eventoId;
    }

    public String getTopico() {
        return topico;
    }

    public Instant getFechaProcesado() {
        return fechaProcesado;
    }
}
