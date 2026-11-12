package com.pedidos360.kafkaadmin.seguridad;

import com.pedidos360.kafkaadmin.config.SecurityConfig;
import com.pedidos360.kafkaadmin.controller.KafkaAdminController;
import com.pedidos360.kafkaadmin.service.KafkaAdminService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Prueba el filtro JWT real de SecurityConfig: sin token se rechaza (401), con token se atiende.
 * El JwtDecoder se reemplaza por un mock para no depender de Azure AD durante el test.
 */
@WebMvcTest(KafkaAdminController.class)
@Import(SecurityConfig.class)
class SeguridadJwtTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private KafkaAdminService adminService;

    @MockBean
    private JwtDecoder jwtDecoder;

    @Test
    void sinTokenLaApiDevuelve401() throws Exception {
        mockMvc.perform(get("/api/kafka/topicos"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void sinTokenNoSePuedeEliminarUnTopico() throws Exception {
        mockMvc.perform(delete("/api/kafka/topicos/pedidos360.ventas.confirmadas"))
                .andExpect(status().isUnauthorized());

        verifyNoInteractions(adminService);
    }

    @Test
    void conJwtValidoLaApiResponde200() throws Exception {
        when(adminService.listarTopicos(false)).thenReturn(List.of());

        mockMvc.perform(get("/api/kafka/topicos").with(jwt()))
                .andExpect(status().isOk());
    }
}
