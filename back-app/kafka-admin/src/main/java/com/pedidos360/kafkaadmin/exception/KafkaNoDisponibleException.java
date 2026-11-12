package com.pedidos360.kafkaadmin.exception;

/** El cluster Kafka no respondio a tiempo o no esta disponible (HTTP 503). */
public class KafkaNoDisponibleException extends RuntimeException {

    public KafkaNoDisponibleException(String mensaje, Throwable causa) {
        super(mensaje, causa);
    }
}
