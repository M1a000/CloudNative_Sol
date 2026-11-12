package com.pedidos360.kafkaadmin.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;

import java.util.Map;

/** Configuraciones a establecer en un topico (solo se modifican las indicadas). */
public record ActualizarConfiguracionesRequest(
        @Schema(description = "Configuraciones a modificar (ej: retention.ms = 172800000)")
        @NotEmpty(message = "debe indicar al menos una configuración")
        Map<@Pattern(regexp = Validaciones.PATRON_CLAVE_CONFIG, message = Validaciones.MENSAJE_CLAVE_CONFIG) String,
            @NotBlank(message = "el valor no puede estar vacío") String> configuraciones) {
}
