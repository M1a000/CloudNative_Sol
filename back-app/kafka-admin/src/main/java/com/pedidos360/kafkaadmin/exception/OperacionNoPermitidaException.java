package com.pedidos360.kafkaadmin.exception;

import org.springframework.http.HttpStatus;

/**
 * La operacion no se puede realizar en el estado actual del cluster:
 * reducir particiones, replicas por sobre los brokers disponibles (400), eliminar un topico interno (400)
 * o resetear offsets de un grupo con consumidores activos (409).
 */
public class OperacionNoPermitidaException extends RuntimeException {

    private final HttpStatus status;

    public OperacionNoPermitidaException(HttpStatus status, String mensaje) {
        super(mensaje);
        this.status = status;
    }

    public HttpStatus getStatus() {
        return status;
    }
}
