package com.pedidos360.kafkaadmin.config;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeType;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.security.SecurityScheme;
import org.springframework.context.annotation.Configuration;

/**
 * Documentacion OpenAPI / Swagger UI del administrador.
 * Disponible en http://localhost:8084/swagger-ui.html
 * (boton "Authorize" para pegar el JWT de Azure AD).
 */
@Configuration
@OpenAPIDefinition(info = @Info(
        title = "Pedidos360 - Administrador de Kafka",
        version = "1.0",
        description = "API REST para crear, modificar, inspeccionar y eliminar tópicos, configuraciones "
                + "y consumer groups del clúster Kafka."))
@SecurityScheme(
        name = OpenApiConfig.ESQUEMA_SEGURIDAD,
        type = SecuritySchemeType.HTTP,
        scheme = "bearer",
        bearerFormat = "JWT")
public class OpenApiConfig {

    public static final String ESQUEMA_SEGURIDAD = "bearerAuth";
}
