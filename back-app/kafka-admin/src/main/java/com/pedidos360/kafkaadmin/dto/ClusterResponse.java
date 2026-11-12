package com.pedidos360.kafkaadmin.dto;

import java.util.List;

/** Estado del cluster: id, broker controlador y brokers activos. */
public record ClusterResponse(String clusterId, Integer controladorId, List<BrokerResponse> brokers) {

    public record BrokerResponse(int id, String host, int puerto, String rack) {
    }
}
