package com.pedidos360.kafkaadmin.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

import java.util.Map;

/** Datos para crear un topico con su configuracion (retencion, cleanup policy, etc.). */
public record CrearTopicoRequest(

        @Schema(description = "Nombre del tópico", example = "pedidos360.notificaciones")
        @NotBlank(message = "es obligatorio")
        @Pattern(regexp = Validaciones.PATRON_TOPICO_NUEVO, message = Validaciones.MENSAJE_TOPICO_NUEVO)
        String nombre,

        @Schema(description = "Cantidad de particiones (paralelismo de consumo)", example = "3")
        @NotNull(message = "es obligatorio")
        @Min(value = 1, message = "debe ser al menos 1")
        @Max(value = Validaciones.MAX_PARTICIONES, message = "no puede superar 50")
        Integer particiones,

        @Schema(description = "Factor de replicación (no puede superar la cantidad de brokers)", example = "3")
        @NotNull(message = "es obligatorio")
        @Min(value = 1, message = "debe ser al menos 1")
        @Max(value = 10, message = "no puede superar 10")
        Integer factorReplicacion,

        @Schema(description = "Configuraciones del tópico (ej: cleanup.policy, retention.ms, min.insync.replicas)")
        Map<@Pattern(regexp = Validaciones.PATRON_CLAVE_CONFIG, message = Validaciones.MENSAJE_CLAVE_CONFIG) String,
            @NotBlank(message = "el valor no puede estar vacío") String> configuraciones) {

    public Map<String, String> configuracionesOVacio() {
        return configuraciones == null ? Map.of() : configuraciones;
    }
}
