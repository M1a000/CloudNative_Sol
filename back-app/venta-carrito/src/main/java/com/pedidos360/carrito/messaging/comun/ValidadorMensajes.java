package com.pedidos360.carrito.messaging.comun;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Valida un evento ya deserializado (anotaciones jakarta.validation del DTO).
 * Si no cumple, lanza {@link MensajeInvalidoException}: error NO recuperable, el mensaje va
 * directo al DLT sin reintentos. Lo usan los consumidores Kafka (los de RabbitMQ usan
 * {@link LectorMensajes}, que ademas deserializa).
 */
@Component
public class ValidadorMensajes {

    private final Validator validator;

    public ValidadorMensajes(Validator validator) {
        this.validator = validator;
    }

    public <T> T validar(T mensaje) {
        if (mensaje == null) {
            throw new MensajeInvalidoException("El mensaje llego vacio");
        }
        Set<ConstraintViolation<T>> errores = validator.validate(mensaje);
        if (!errores.isEmpty()) {
            String detalle = errores.stream()
                    .map(error -> error.getPropertyPath() + " " + error.getMessage())
                    .sorted()
                    .collect(Collectors.joining("; "));
            throw new MensajeInvalidoException("Datos faltantes o invalidos: " + detalle);
        }
        return mensaje;
    }
}
