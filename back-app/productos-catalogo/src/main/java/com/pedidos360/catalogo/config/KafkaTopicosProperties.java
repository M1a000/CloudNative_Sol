package com.pedidos360.catalogo.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.Map;

/**
 * Topologia Kafka leida desde application.yml (prefijo pedidos360.kafka).
 * Ninguna clase escribe nombres de topicos o grupos "a mano": todas usan este objeto.
 * Si falta alguna propiedad obligatoria, la aplicacion no arranca.
 */
@Validated
@ConfigurationProperties(prefix = "pedidos360.kafka")
public record KafkaTopicosProperties(
        @Valid @NotNull Topicos topicos,
        @Valid @NotNull Dlt dlt,
        @Valid @NotNull Consumidores consumidores,
        @Valid @NotNull Reintentos reintentos) {

    public record Topicos(@Valid @NotNull Topico inventarioActualizado,
                          @Valid @NotNull Topico ventasConfirmadas) {
    }

    /**
     * Un topico. Particiones, replicas y configuraciones solo se indican para los topicos
     * que este microservicio produce (es su "dueño" y los declara al arrancar).
     */
    public record Topico(@NotBlank String nombre,
                         Integer particiones,
                         Integer replicas,
                         Map<String, String> configuraciones) {

        public Map<String, String> configuracionesOVacio() {
            return configuraciones == null ? Map.of() : configuraciones;
        }
    }

    /** Dead Letter Topic: nombre = topico original + sufijo. */
    public record Dlt(@NotBlank String sufijo,
                      @Positive int particiones,
                      @Positive int replicas,
                      Map<String, String> configuraciones) {

        public String nombrePara(String topico) {
            return topico + sufijo;
        }

        public Map<String, String> configuracionesOVacio() {
            return configuraciones == null ? Map.of() : configuraciones;
        }
    }

    public record Consumidores(@Valid @NotNull Consumidor metricasVentas) {
    }

    /** Consumer group y cantidad de hilos consumidores (idealmente = numero de particiones). */
    public record Consumidor(@NotBlank String grupo, @Min(1) int concurrencia) {
    }

    /** Reintentos para errores recuperables, con backoff exponencial. */
    public record Reintentos(@Min(1) int maxIntentos,
                             @Positive long intervaloInicialMs,
                             @DecimalMin("1.0") double multiplicador,
                             @Positive long intervaloMaximoMs) {
    }
}
