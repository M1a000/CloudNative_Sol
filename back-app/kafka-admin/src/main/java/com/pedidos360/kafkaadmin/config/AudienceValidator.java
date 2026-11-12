package com.pedidos360.kafkaadmin.config;

import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.Jwt;

import java.util.List;

/**
 * Verifica que el JWT haya sido emitido para ESTA aplicacion (audience),
 * y no para otra app registrada en el mismo tenant de Azure AD.
 */
public class AudienceValidator implements OAuth2TokenValidator<Jwt> {

    private final String expectedAudience;

    public AudienceValidator(String expectedAudience) {
        this.expectedAudience = expectedAudience;
    }

    @Override
    public OAuth2TokenValidatorResult validate(Jwt jwt) {
        List<String> audiences = jwt.getAudience();

        if (audiences != null && audiences.contains(expectedAudience)) {
            return OAuth2TokenValidatorResult.success();
        }

        OAuth2Error error = new OAuth2Error(
                "invalid_token",
                "El token no contiene el audience esperado (" + expectedAudience + ")",
                null
        );
        return OAuth2TokenValidatorResult.failure(error);
    }
}
