package com.pedidos360.kafkaadmin.service;

import com.pedidos360.kafkaadmin.dto.ClusterResponse;
import com.pedidos360.kafkaadmin.dto.CrearTopicoRequest;
import com.pedidos360.kafkaadmin.dto.EstrategiaReset;
import com.pedidos360.kafkaadmin.dto.GrupoDetalleResponse;
import com.pedidos360.kafkaadmin.dto.ResetOffsetsRequest;
import com.pedidos360.kafkaadmin.dto.TopicoDetalleResponse;
import com.pedidos360.kafkaadmin.exception.OperacionNoPermitidaException;
import com.pedidos360.kafkaadmin.exception.RecursoNoEncontradoException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Prueba de humo del administrador contra el CLUSTER REAL levantado con docker compose (3 brokers).
 * No corre en el build normal; ejecutarla con:
 * <pre>
 *   mvn -pl kafka-admin test -Dkafka.local=true -Dtest=KafkaAdminServiceClusterLocalTest
 * </pre>
 * Crea un topico temporal, lo modifica, lo inspecciona y lo elimina.
 */
@EnabledIfSystemProperty(named = "kafka.local", matches = "true")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KafkaAdminServiceClusterLocalTest {

    private static final String BOOTSTRAP = System.getProperty("kafka.bootstrap",
            "localhost:9092,localhost:9093,localhost:9094");
    private static final String TOPICO = "pedidos360.prueba-admin";
    private static final String GRUPO = "prueba-admin-grupo";

    private static AdminClient admin;
    private static KafkaAdminService servicio;

    @BeforeAll
    static void conectar() {
        admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP));
        servicio = new KafkaAdminService(admin, 20);
        try {
            servicio.eliminarTopico(TOPICO); // restos de una ejecucion anterior
        } catch (RecursoNoEncontradoException | UnknownTopicOrPartitionException ignorado) {
            // no existia
        }
    }

    @AfterAll
    static void cerrar() {
        admin.close();
    }

    @Test
    @Order(1)
    void elClusterTieneTresBrokers() {
        ClusterResponse cluster = servicio.obtenerCluster();

        assertThat(cluster.brokers()).extracting(ClusterResponse.BrokerResponse::id).containsExactly(1, 2, 3);
        assertThat(cluster.controladorId()).isNotNull();
    }

    @Test
    @Order(2)
    void creaUnTopicoConParticionesReplicasYConfiguraciones() {
        TopicoDetalleResponse creado = servicio.crearTopico(new CrearTopicoRequest(TOPICO, 2, 3,
                Map.of("cleanup.policy", "delete", "retention.ms", "3600000", "min.insync.replicas", "2")));

        assertThat(creado.particiones()).isEqualTo(2);
        assertThat(creado.factorReplicacion()).isEqualTo(3);
        assertThat(creado.configuracionesModificadas())
                .containsEntry("retention.ms", "3600000")
                .containsEntry("min.insync.replicas", "2");
        assertThat(creado.detalleParticiones()).allSatisfy(p -> assertThat(p.isr()).hasSize(3));
    }

    @Test
    @Order(3)
    void crearDosVecesElMismoTopicoFalla() {
        assertThatThrownBy(() -> servicio.crearTopico(new CrearTopicoRequest(TOPICO, 1, 1, null)))
                .isInstanceOf(TopicExistsException.class);
    }

    @Test
    @Order(4)
    void rechazaMasReplicasQueBrokers() {
        assertThatThrownBy(() -> servicio.crearTopico(new CrearTopicoRequest("pedidos360.prueba-rf", 1, 5, null)))
                .isInstanceOf(OperacionNoPermitidaException.class)
                .hasMessageContaining("3");
    }

    @Test
    @Order(5)
    void modificaYRestableceConfiguraciones() {
        TopicoDetalleResponse modificado = servicio.modificarConfiguraciones(TOPICO, Map.of("retention.ms", "7200000"));
        assertThat(modificado.configuracionesModificadas()).containsEntry("retention.ms", "7200000");

        TopicoDetalleResponse restablecido = servicio.restablecerConfiguracion(TOPICO, "retention.ms");
        assertThat(restablecido.configuracionesModificadas()).doesNotContainKey("retention.ms");
    }

    @Test
    @Order(6)
    void aumentaParticionesPeroNoPermiteReducirlas() {
        assertThat(servicio.aumentarParticiones(TOPICO, 4).particiones()).isEqualTo(4);

        assertThatThrownBy(() -> servicio.aumentarParticiones(TOPICO, 3))
                .isInstanceOf(OperacionNoPermitidaException.class);
    }

    @Test
    @Order(7)
    void inspeccionaYReseteaOffsetsDeUnConsumerGroup() {
        Properties productor = new Properties();
        productor.put("bootstrap.servers", BOOTSTRAP);
        try (KafkaProducer<String, String> p = new KafkaProducer<>(productor, new StringSerializer(), new StringSerializer())) {
            for (int i = 0; i < 6; i++) {
                p.send(new ProducerRecord<>(TOPICO, "k" + i, "mensaje " + i));
            }
        }
        Properties consumidor = new Properties();
        consumidor.put("bootstrap.servers", BOOTSTRAP);
        consumidor.put("group.id", GRUPO);
        consumidor.put("auto.offset.reset", "earliest");
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(consumidor, new StringDeserializer(), new StringDeserializer())) {
            c.subscribe(List.of(TOPICO));
            int leidos = 0;
            for (int intento = 0; intento < 20 && leidos < 6; intento++) {
                leidos += c.poll(Duration.ofMillis(500)).count();
            }
            c.commitSync();
        }

        GrupoDetalleResponse grupo = servicio.obtenerGrupo(GRUPO);
        assertThat(grupo.lagTotal()).isZero();
        assertThat(grupo.miembros()).isEmpty(); // el consumidor ya se cerro

        GrupoDetalleResponse reseteado = servicio.resetearOffsets(GRUPO,
                new ResetOffsetsRequest(TOPICO, EstrategiaReset.EARLIEST));
        assertThat(reseteado.lagTotal()).isEqualTo(6);

        servicio.eliminarGrupo(GRUPO);
        assertThatThrownBy(() -> servicio.obtenerGrupo(GRUPO)).isInstanceOf(RecursoNoEncontradoException.class);
    }

    @Test
    @Order(8)
    void noPermiteEliminarTopicosInternosYEliminaElTemporal() {
        assertThatThrownBy(() -> servicio.eliminarTopico("__consumer_offsets"))
                .isInstanceOf(OperacionNoPermitidaException.class);

        servicio.eliminarTopico(TOPICO);
        assertThat(servicio.listarTopicos(false)).noneMatch(t -> t.nombre().equals(TOPICO));
    }
}
