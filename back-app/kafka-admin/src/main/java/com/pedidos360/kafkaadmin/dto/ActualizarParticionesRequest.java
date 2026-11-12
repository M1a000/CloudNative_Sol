package com.pedidos360.kafkaadmin.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

/** Nuevo total de particiones. Kafka solo permite AUMENTARLAS (no se pueden quitar). */
public record ActualizarParticionesRequest(
        @Schema(description = "Nuevo total de particiones (mayor al actual)", example = "6")
        @NotNull(message = "es obligatorio")
        @Min(value = 2, message = "debe ser al menos 2")
        @Max(value = Validaciones.MAX_PARTICIONES, message = "no puede superar 50")
        Integer particiones) {
}
