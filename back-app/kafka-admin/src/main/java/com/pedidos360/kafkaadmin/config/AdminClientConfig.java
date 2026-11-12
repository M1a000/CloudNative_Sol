package com.pedidos360.kafkaadmin.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * AdminClient de Kafka: cliente oficial para operaciones de administracion del cluster.
 * Toma bootstrap servers y timeouts de spring.kafka.admin.* (application.yml).
 */
@Configuration
public class AdminClientConfig {

    @Bean(destroyMethod = "close")
    public AdminClient adminClient(KafkaAdmin kafkaAdmin) {
        return AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }
}
