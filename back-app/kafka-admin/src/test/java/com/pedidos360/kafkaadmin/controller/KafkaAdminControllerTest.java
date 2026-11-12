package com.pedidos360.kafkaadmin.controller;

import com.pedidos360.kafkaadmin.dto.CrearTopicoRequest;
import com.pedidos360.kafkaadmin.dto.TopicoDetalleResponse;
import com.pedidos360.kafkaadmin.dto.TopicoResumenResponse;
import com.pedidos360.kafkaadmin.service.KafkaAdminService;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Pruebas de la API REST del administrador de Kafka: rutas, codigos HTTP y validacion de entrada.
 * Se desactivan los filtros de seguridad (JWT) para probar solo la capa web.
 */
@WebMvcTest(KafkaAdminController.class)
@AutoConfigureMockMvc(addFilters = false)
class KafkaAdminControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private KafkaAdminService adminService;

    @Test
    void listarTopicosDevuelve200ConLaLista() throws Exception {
        when(adminService.listarTopicos(false)).thenReturn(List.of(
                new TopicoResumenResponse("pedidos360.ventas.confirmadas", 3, 3, false)));

        mockMvc.perform(get("/api/kafka/topicos"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].nombre").value("pedidos360.ventas.confirmadas"))
                .andExpect(jsonPath("$[0].particiones").value(3));
    }

    @Test
    void crearTopicoValidoDevuelve201ConLocation() throws Exception {
        when(adminService.crearTopico(any(CrearTopicoRequest.class))).thenReturn(new TopicoDetalleResponse(
                "pedidos360.pruebas", false, 3, 3, List.of(), Map.of("retention.ms", "86400000")));

        mockMvc.perform(post("/api/kafka/topicos")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"nombre": "pedidos360.pruebas", "particiones": 3, "factorReplicacion": 3,
                                 "configuraciones": {"retention.ms": "86400000"}}
                                """))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", "/api/kafka/topicos/pedidos360.pruebas"))
                .andExpect(jsonPath("$.configuracionesModificadas['retention.ms']").value("86400000"));
    }

    @Test
    void crearTopicoConDatosInvalidosDevuelve400SinLlamarAKafka() throws Exception {
        mockMvc.perform(post("/api/kafka/topicos")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"nombre": "__interno", "particiones": 0, "factorReplicacion": 3,
                                 "configuraciones": {"Retention.MS": "1"}}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detalles.length()").value(3));

        verifyNoInteractions(adminService);
    }

    @Test
    void crearTopicoExistenteDevuelve409() throws Exception {
        when(adminService.crearTopico(any(CrearTopicoRequest.class)))
                .thenThrow(new TopicExistsException("Topic 'pedidos360.pruebas' already exists."));

        mockMvc.perform(post("/api/kafka/topicos")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"nombre\": \"pedidos360.pruebas\", \"particiones\": 1, \"factorReplicacion\": 1}"))
                .andExpect(status().isConflict());
    }

    @Test
    void obtenerTopicoInexistenteDevuelve404() throws Exception {
        when(adminService.obtenerTopico("no.existe"))
                .thenThrow(new UnknownTopicOrPartitionException("This server does not host this topic-partition."));

        mockMvc.perform(get("/api/kafka/topicos/no.existe"))
                .andExpect(status().isNotFound());
    }

    @Test
    void eliminarTopicoDevuelve204() throws Exception {
        mockMvc.perform(delete("/api/kafka/topicos/pedidos360.pruebas"))
                .andExpect(status().isNoContent());

        verify(adminService).eliminarTopico("pedidos360.pruebas");
    }

    @Test
    void resetOffsetsConEstrategiaInvalidaDevuelve400() throws Exception {
        mockMvc.perform(post("/api/kafka/grupos/catalogo-metricas-ventas/reset-offsets")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topico\": \"pedidos360.ventas.confirmadas\", \"estrategia\": \"AYER\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detalles[0]").value(org.hamcrest.Matchers.containsString("EARLIEST")));
    }

    @Test
    void eliminarGrupoConConsumidoresDevuelve409() throws Exception {
        doThrow(new org.apache.kafka.common.errors.GroupNotEmptyException("The group is not empty."))
                .when(adminService).eliminarGrupo("carrito-sync-inventario");

        mockMvc.perform(delete("/api/kafka/grupos/carrito-sync-inventario"))
                .andExpect(status().isConflict());
    }
}
