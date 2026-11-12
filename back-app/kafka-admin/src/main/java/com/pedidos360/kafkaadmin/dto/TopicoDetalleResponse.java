package com.pedidos360.kafkaadmin.dto;

import java.util.List;
import java.util.Map;

/**
 * Detalle de un topico: particiones con su lider, replicas e ISR, y las configuraciones que
 * difieren del valor por defecto del broker (retencion, cleanup policy, min.insync.replicas...).
 */
public record TopicoDetalleResponse(
        String nombre,
        boolean interno,
        int particiones,
        int factorReplicacion,
        List<ParticionResponse> detalleParticiones,
        Map<String, String> configuracionesModificadas) {

    /** subReplicada = true si alguna replica no esta sincronizada (ISR menor que replicas). */
    public record ParticionResponse(int particion, Integer lider, List<Integer> replicas, List<Integer> isr,
                                    boolean subReplicada) {
    }
}
