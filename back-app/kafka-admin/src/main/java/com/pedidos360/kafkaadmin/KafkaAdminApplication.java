package com.pedidos360.kafkaadmin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Microservicio administrador de Kafka.
 * Expone una API REST para crear, modificar, inspeccionar y eliminar topicos, sus configuraciones
 * y los consumer groups, sin tener que entrar a Kafka-UI ni usar la consola de los brokers.
 */
@SpringBootApplication
public class KafkaAdminApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaAdminApplication.class, args);
    }
}
