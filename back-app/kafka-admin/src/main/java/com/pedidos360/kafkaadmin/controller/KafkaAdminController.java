package com.pedidos360.kafkaadmin.controller;

import com.pedidos360.kafkaadmin.config.OpenApiConfig;
import com.pedidos360.kafkaadmin.dto.ActualizarConfiguracionesRequest;
import com.pedidos360.kafkaadmin.dto.ActualizarParticionesRequest;
import com.pedidos360.kafkaadmin.dto.ClusterResponse;
import com.pedidos360.kafkaadmin.dto.ConfiguracionResponse;
import com.pedidos360.kafkaadmin.dto.CrearTopicoRequest;
import com.pedidos360.kafkaadmin.dto.GrupoDetalleResponse;
import com.pedidos360.kafkaadmin.dto.GrupoResumenResponse;
import com.pedidos360.kafkaadmin.dto.ResetOffsetsRequest;
import com.pedidos360.kafkaadmin.dto.TopicoDetalleResponse;
import com.pedidos360.kafkaadmin.dto.TopicoResumenResponse;
import com.pedidos360.kafkaadmin.dto.Validaciones;
import com.pedidos360.kafkaadmin.service.KafkaAdminService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.List;

/**
 * API REST del administrador de Kafka.
 *
 * <pre>
 * GET    /api/kafka/cluster                                   brokers y controlador
 * GET    /api/kafka/topicos?incluirInternos=false             listar topicos
 * POST   /api/kafka/topicos                                   crear topico con particiones, replicas y configs
 * GET    /api/kafka/topicos/{nombre}                          particiones (lider, replicas, ISR) y configs propias
 * PATCH  /api/kafka/topicos/{nombre}/particiones              aumentar particiones
 * GET    /api/kafka/topicos/{nombre}/configuraciones          todas las configuraciones con su origen
 * PUT    /api/kafka/topicos/{nombre}/configuraciones          modificar configuraciones (retention.ms, cleanup.policy...)
 * DELETE /api/kafka/topicos/{nombre}/configuraciones/{clave}  volver una configuracion al valor por defecto
 * DELETE /api/kafka/topicos/{nombre}                          eliminar topico
 * GET    /api/kafka/grupos                                    listar consumer groups
 * GET    /api/kafka/grupos/{groupId}                          miembros, particiones asignadas, offsets y lag
 * POST   /api/kafka/grupos/{groupId}/reset-offsets            resetear offsets (reprocesar o saltar)
 * DELETE /api/kafka/grupos/{groupId}                          eliminar consumer group
 * </pre>
 *
 * El controlador solo valida la entrada y delega en {@link KafkaAdminService}.
 */
@RestController
@RequestMapping("/api/kafka")
@Validated
@Tag(name = "Administrador Kafka", description = "Tópicos, configuraciones y consumer groups")
@SecurityRequirement(name = OpenApiConfig.ESQUEMA_SEGURIDAD)
public class KafkaAdminController {

    private final KafkaAdminService adminService;

    public KafkaAdminController(KafkaAdminService adminService) {
        this.adminService = adminService;
    }

    // ===================================================================== CLUSTER

    @Operation(summary = "Estado del clúster", description = "Brokers activos y broker controlador")
    @GetMapping("/cluster")
    public ResponseEntity<ClusterResponse> obtenerCluster() {
        return ResponseEntity.ok(adminService.obtenerCluster());
    }

    // ===================================================================== TOPICOS

    @Operation(summary = "Listar tópicos")
    @GetMapping("/topicos")
    public ResponseEntity<List<TopicoResumenResponse>> listarTopicos(
            @RequestParam(defaultValue = "false") boolean incluirInternos) {
        return ResponseEntity.ok(adminService.listarTopicos(incluirInternos));
    }

    @Operation(summary = "Crear un tópico", description = "Con particiones, factor de replicación y configuraciones")
    @PostMapping("/topicos")
    public ResponseEntity<TopicoDetalleResponse> crearTopico(@Valid @RequestBody CrearTopicoRequest request) {
        TopicoDetalleResponse creado = adminService.crearTopico(request);
        return ResponseEntity.created(URI.create("/api/kafka/topicos/" + request.nombre())).body(creado);
    }

    @Operation(summary = "Detalle de un tópico", description = "Particiones (líder, réplicas, ISR) y configuraciones propias")
    @GetMapping("/topicos/{nombre}")
    public ResponseEntity<TopicoDetalleResponse> obtenerTopico(
            @PathVariable("nombre")
            @Pattern(regexp = Validaciones.PATRON_TOPICO, message = Validaciones.MENSAJE_TOPICO) String nombre) {
        return ResponseEntity.ok(adminService.obtenerTopico(nombre));
    }

    @Operation(summary = "Aumentar particiones", description = "Kafka no permite reducirlas")
    @PatchMapping("/topicos/{nombre}/particiones")
    public ResponseEntity<TopicoDetalleResponse> aumentarParticiones(
            @PathVariable("nombre")
            @Pattern(regexp = Validaciones.PATRON_TOPICO, message = Validaciones.MENSAJE_TOPICO) String nombre,
            @Valid @RequestBody ActualizarParticionesRequest request) {
        return ResponseEntity.ok(adminService.aumentarParticiones(nombre, request.particiones()));
    }

    @Operation(summary = "Eliminar un tópico", description = "No permite eliminar tópicos internos (__*)")
    @DeleteMapping("/topicos/{nombre}")
    public ResponseEntity<Void> eliminarTopico(
            @PathVariable("nombre")
            @Pattern(regexp = Validaciones.PATRON_TOPICO, message = Validaciones.MENSAJE_TOPICO) String nombre) {
        adminService.eliminarTopico(nombre);
        return ResponseEntity.noContent().build();
    }

    // ============================================================ CONFIGURACIONES

    @Operation(summary = "Configuraciones de un tópico", description = "Valor efectivo y origen de cada configuración")
    @GetMapping("/topicos/{nombre}/configuraciones")
    public ResponseEntity<List<ConfiguracionResponse>> listarConfiguraciones(
            @PathVariable("nombre")
            @Pattern(regexp = Validaciones.PATRON_TOPICO, message = Validaciones.MENSAJE_TOPICO) String nombre) {
        return ResponseEntity.ok(adminService.listarConfiguraciones(nombre));
    }

    @Operation(summary = "Modificar configuraciones", description = "Ej: retention.ms, cleanup.policy, min.insync.replicas")
    @PutMapping("/topicos/{nombre}/configuraciones")
    public ResponseEntity<TopicoDetalleResponse> modificarConfiguraciones(
            @PathVariable("nombre")
            @Pattern(regexp = Validaciones.PATRON_TOPICO, message = Validaciones.MENSAJE_TOPICO) String nombre,
            @Valid @RequestBody ActualizarConfiguracionesRequest request) {
        return ResponseEntity.ok(adminService.modificarConfiguraciones(nombre, request.configuraciones()));
    }

    @Operation(summary = "Restablecer una configuración", description = "Vuelve al valor por defecto del broker")
    @DeleteMapping("/topicos/{nombre}/configuraciones/{clave}")
    public ResponseEntity<TopicoDetalleResponse> restablecerConfiguracion(
            @PathVariable("nombre")
            @Pattern(regexp = Validaciones.PATRON_TOPICO, message = Validaciones.MENSAJE_TOPICO) String nombre,
            @PathVariable("clave")
            @Pattern(regexp = Validaciones.PATRON_CLAVE_CONFIG, message = Validaciones.MENSAJE_CLAVE_CONFIG) String clave) {
        return ResponseEntity.ok(adminService.restablecerConfiguracion(nombre, clave));
    }

    // ============================================================ CONSUMER GROUPS

    @Operation(summary = "Listar consumer groups")
    @GetMapping("/grupos")
    public ResponseEntity<List<GrupoResumenResponse>> listarGrupos() {
        return ResponseEntity.ok(adminService.listarGrupos());
    }

    @Operation(summary = "Detalle de un consumer group", description = "Miembros, particiones asignadas, offsets y lag")
    @GetMapping("/grupos/{groupId}")
    public ResponseEntity<GrupoDetalleResponse> obtenerGrupo(
            @PathVariable("groupId")
            @Pattern(regexp = Validaciones.PATRON_GRUPO, message = Validaciones.MENSAJE_GRUPO) String groupId) {
        return ResponseEntity.ok(adminService.obtenerGrupo(groupId));
    }

    @Operation(summary = "Resetear offsets de un grupo",
            description = "EARLIEST reprocesa los mensajes retenidos; LATEST salta lo pendiente. El grupo debe estar sin consumidores")
    @PostMapping("/grupos/{groupId}/reset-offsets")
    public ResponseEntity<GrupoDetalleResponse> resetearOffsets(
            @PathVariable("groupId")
            @Pattern(regexp = Validaciones.PATRON_GRUPO, message = Validaciones.MENSAJE_GRUPO) String groupId,
            @Valid @RequestBody ResetOffsetsRequest request) {
        return ResponseEntity.ok(adminService.resetearOffsets(groupId, request));
    }

    @Operation(summary = "Eliminar un consumer group", description = "El grupo debe estar sin consumidores activos")
    @DeleteMapping("/grupos/{groupId}")
    public ResponseEntity<Void> eliminarGrupo(
            @PathVariable("groupId")
            @Pattern(regexp = Validaciones.PATRON_GRUPO, message = Validaciones.MENSAJE_GRUPO) String groupId) {
        adminService.eliminarGrupo(groupId);
        return ResponseEntity.noContent().build();
    }
}
