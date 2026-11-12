package com.pedidos360.carrito.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.LocalDateTime;
import java.util.Map;

/** Respuestas HTTP claras para los errores del flujo de confirmacion de pedidos. */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(CarritoVacioException.class)
    public ResponseEntity<Map<String, Object>> manejarCarritoVacio(CarritoVacioException ex) {
        return responder(HttpStatus.UNPROCESSABLE_ENTITY, ex.getMessage());
    }

    /** Hay productos agotados en el carrito (disponibilidad sincronizada desde Kafka). */
    @ExceptionHandler(ProductoSinStockException.class)
    public ResponseEntity<Map<String, Object>> manejarSinStock(ProductoSinStockException ex) {
        return responder(HttpStatus.CONFLICT, ex.getMessage());
    }

    /** RabbitMQ no disponible al publicar: el carrito no se vacia (rollback) y se informa al cliente. */
    @ExceptionHandler(AmqpException.class)
    public ResponseEntity<Map<String, Object>> manejarMensajeriaNoDisponible(AmqpException ex) {
        log.error("No se pudo publicar el evento en RabbitMQ: {}", ex.getMessage());
        return responder(HttpStatus.SERVICE_UNAVAILABLE,
                "El sistema de pedidos no está disponible en este momento. Intente nuevamente.");
    }

    private ResponseEntity<Map<String, Object>> responder(HttpStatus status, String mensaje) {
        Map<String, Object> cuerpo = Map.of(
                "timestamp", LocalDateTime.now().toString(),
                "status", status.value(),
                "error", mensaje);
        return ResponseEntity.status(status).body(cuerpo);
    }
}
