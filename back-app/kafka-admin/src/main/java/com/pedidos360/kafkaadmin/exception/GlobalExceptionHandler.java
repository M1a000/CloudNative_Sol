package com.pedidos360.kafkaadmin.exception;

import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.pedidos360.kafkaadmin.dto.RespuestaError;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Traduce las excepciones (propias y del AdminClient de Kafka) a respuestas HTTP claras
 * con el formato {@link RespuestaError}.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    // ===================================================== VALIDACION DE ENTRADA

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<RespuestaError> manejarBodyInvalido(MethodArgumentNotValidException ex,
                                                             HttpServletRequest request) {
        List<String> detalles = new ArrayList<>();
        ex.getBindingResult().getFieldErrors()
                .forEach(error -> detalles.add(error.getField() + ": " + error.getDefaultMessage()));
        ex.getBindingResult().getGlobalErrors()
                .forEach(error -> detalles.add(error.getDefaultMessage()));
        detalles.sort(String::compareTo);
        return responder(HttpStatus.BAD_REQUEST, "Datos de entrada inválidos", detalles, request);
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<RespuestaError> manejarParametroInvalido(ConstraintViolationException ex,
                                                                  HttpServletRequest request) {
        List<String> detalles = new ArrayList<>();
        for (ConstraintViolation<?> violacion : ex.getConstraintViolations()) {
            String ruta = violacion.getPropertyPath().toString();
            String parametro = ruta.substring(ruta.lastIndexOf('.') + 1);
            detalles.add(parametro + ": " + violacion.getMessage());
        }
        detalles.sort(String::compareTo);
        return responder(HttpStatus.BAD_REQUEST, "Parámetros inválidos", detalles, request);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<RespuestaError> manejarJsonInvalido(HttpMessageNotReadableException ex,
                                                             HttpServletRequest request) {
        String detalle = "El cuerpo de la petición no es un JSON válido";
        if (ex.getCause() instanceof InvalidFormatException formato
                && formato.getTargetType() != null
                && formato.getTargetType().isEnum()
                && !formato.getPath().isEmpty()) {
            String campo = formato.getPath().get(formato.getPath().size() - 1).getFieldName();
            detalle = campo + ": el valor '" + formato.getValue() + "' no es válido. Valores permitidos: "
                    + Arrays.toString(formato.getTargetType().getEnumConstants());
        }
        return responder(HttpStatus.BAD_REQUEST, "Datos de entrada inválidos", List.of(detalle), request);
    }

    // ===================================================== ERRORES PROPIOS

    @ExceptionHandler(RecursoNoEncontradoException.class)
    public ResponseEntity<RespuestaError> manejarNoEncontrado(RecursoNoEncontradoException ex,
                                                             HttpServletRequest request) {
        return responder(HttpStatus.NOT_FOUND, "Recurso no encontrado", List.of(ex.getMessage()), request);
    }

    @ExceptionHandler(OperacionNoPermitidaException.class)
    public ResponseEntity<RespuestaError> manejarNoPermitida(OperacionNoPermitidaException ex,
                                                            HttpServletRequest request) {
        return responder(ex.getStatus(), "Operación no permitida", List.of(ex.getMessage()), request);
    }

    @ExceptionHandler(KafkaNoDisponibleException.class)
    public ResponseEntity<RespuestaError> manejarKafkaNoDisponible(KafkaNoDisponibleException ex,
                                                                  HttpServletRequest request) {
        log.error("Kafka no disponible: {}", ex.getMessage());
        return responder(HttpStatus.SERVICE_UNAVAILABLE, "Kafka no disponible",
                List.of("No se pudo comunicar con el clúster. Verifique que los brokers estén levantados."), request);
    }

    // ===================================================== ERRORES DEL ADMINCLIENT DE KAFKA

    @ExceptionHandler(TopicExistsException.class)
    public ResponseEntity<RespuestaError> manejarTopicoExistente(TopicExistsException ex, HttpServletRequest request) {
        return responder(HttpStatus.CONFLICT, "El recurso ya existe", List.of(ex.getMessage()), request);
    }

    @ExceptionHandler({UnknownTopicOrPartitionException.class, GroupIdNotFoundException.class})
    public ResponseEntity<RespuestaError> manejarNoExisteEnKafka(ApiException ex, HttpServletRequest request) {
        return responder(HttpStatus.NOT_FOUND, "Recurso no encontrado", List.of(ex.getMessage()), request);
    }

    @ExceptionHandler({GroupNotEmptyException.class, UnknownMemberIdException.class})
    public ResponseEntity<RespuestaError> manejarGrupoActivo(ApiException ex, HttpServletRequest request) {
        return responder(HttpStatus.CONFLICT, "Operación no permitida",
                List.of("El consumer group tiene consumidores activos: deténgalos antes de esta operación. ("
                        + ex.getMessage() + ")"), request);
    }

    /** Kafka rechazo los parametros (particiones, replicas, configuracion o nombre invalidos). */
    @ExceptionHandler({InvalidPartitionsException.class, InvalidReplicationFactorException.class,
            InvalidConfigurationException.class, InvalidRequestException.class, PolicyViolationException.class,
            InvalidTopicException.class})
    public ResponseEntity<RespuestaError> manejarParametrosRechazados(ApiException ex, HttpServletRequest request) {
        return responder(HttpStatus.BAD_REQUEST, "Kafka rechazó los parámetros", List.of(ex.getMessage()), request);
    }

    @ExceptionHandler(TimeoutException.class)
    public ResponseEntity<RespuestaError> manejarTimeout(TimeoutException ex, HttpServletRequest request) {
        log.error("Timeout del AdminClient: {}", ex.getMessage());
        return responder(HttpStatus.SERVICE_UNAVAILABLE, "Kafka no disponible", List.of(ex.getMessage()), request);
    }

    /** Cualquier otro error devuelto por el cluster. */
    @ExceptionHandler(ApiException.class)
    public ResponseEntity<RespuestaError> manejarErrorKafka(ApiException ex, HttpServletRequest request) {
        log.error("Kafka rechazó la operación: {}", ex.getMessage(), ex);
        return responder(HttpStatus.BAD_GATEWAY, "Kafka rechazó la operación", List.of(ex.getMessage()), request);
    }

    private ResponseEntity<RespuestaError> responder(HttpStatus status, String error, List<String> detalles,
                                                     HttpServletRequest request) {
        RespuestaError cuerpo = new RespuestaError(
                LocalDateTime.now(), status.value(), error, detalles, request.getRequestURI());
        return ResponseEntity.status(status).body(cuerpo);
    }
}
