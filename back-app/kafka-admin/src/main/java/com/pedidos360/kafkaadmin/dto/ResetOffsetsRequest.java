package com.pedidos360.kafkaadmin.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

/** Reseteo de offsets de un consumer group sobre un topico (el grupo debe estar sin consumidores activos). */
public record ResetOffsetsRequest(
        @Schema(description = "Tópico cuyos offsets se resetean", example = "pedidos360.ventas.confirmadas")
        @NotBlank(message = "es obligatorio")
        @Pattern(regexp = Validaciones.PATRON_TOPICO, message = Validaciones.MENSAJE_TOPICO)
        String topico,

        @Schema(description = "EARLIEST (reprocesar todo) o LATEST (saltar lo pendiente)", example = "EARLIEST")
        @NotNull(message = "es obligatorio")
        EstrategiaReset estrategia) {
}
