package com.pedidos360.kafkaadmin.dto;

import java.util.List;

/**
 * Detalle de un consumer group: miembros activos con sus particiones asignadas
 * y, por particion, offset confirmado, ultimo offset del topico y lag (mensajes pendientes).
 */
public record GrupoDetalleResponse(
        String groupId,
        String estado,
        String asignador,
        Integer coordinador,
        List<MiembroResponse> miembros,
        List<OffsetParticionResponse> offsets,
        long lagTotal) {

    public record MiembroResponse(String memberId, String clientId, String host, List<String> particionesAsignadas) {
    }

    /** offsetConfirmado y lag son null si el grupo nunca confirmo un offset en esa particion. */
    public record OffsetParticionResponse(String topico, int particion, Long offsetConfirmado, long offsetFinal,
                                          Long lag) {
    }
}
