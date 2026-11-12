package com.pedidos360.kafkaadmin.dto;

/**
 * Reglas de validacion compartidas por los DTO y los parametros de ruta del administrador.
 * Estan centralizadas aqui para que POST, PUT y DELETE validen exactamente lo mismo.
 */
public final class Validaciones {

    private Validaciones() {
    }

    /** Nombre legal de topico en Kafka (lectura: incluye los internos como __consumer_offsets). */
    public static final String PATRON_TOPICO = "^[a-zA-Z0-9._-]{1,249}$";

    public static final String MENSAJE_TOPICO =
            "solo admite letras, números, '.', '_' y '-' (máximo 249 caracteres)";

    /** Topico nuevo: 3 a 249 caracteres y no puede empezar con "__" (reservado para topicos internos). */
    public static final String PATRON_TOPICO_NUEVO = "^(?!__)[a-zA-Z0-9._-]{3,249}$";

    public static final String MENSAJE_TOPICO_NUEVO =
            "debe tener entre 3 y 249 caracteres (letras, números, '.', '_' o '-') "
            + "y no puede empezar con '__' (reservado por Kafka)";

    /** Identificador de consumer group. */
    public static final String PATRON_GRUPO = "^[a-zA-Z0-9._-]{1,255}$";

    public static final String MENSAJE_GRUPO = "solo admite letras, números, '.', '_' y '-' (máximo 255)";

    /** Clave de configuracion de topico, por ejemplo retention.ms o cleanup.policy. */
    public static final String PATRON_CLAVE_CONFIG = "^[a-z][a-z0-9.]{2,100}$";

    public static final String MENSAJE_CLAVE_CONFIG =
            "debe ser una clave de configuración de Kafka en minúsculas (ej: retention.ms, cleanup.policy)";

    public static final int MAX_PARTICIONES = 50;
}
