package com.pedidos360.catalogo.config;

import com.pedidos360.catalogo.exception.ReglaNegocioException;
import com.pedidos360.catalogo.messaging.comun.MensajeInvalidoException;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Configuracion Kafka de productos-catalogo.
 *
 * <pre>
 *  PRODUCE   pedidos360.inventario.actualizado   (compact, key = productoId)
 *  CONSUME   pedidos360.ventas.confirmadas       (grupo catalogo-metricas-ventas, 3 hilos)
 *  DLT       pedidos360.ventas.confirmadas.DLT   (mensajes de ventas que no se pudieron procesar)
 * </pre>
 *
 * Los topicos tambien los crea el script docker/kafka/crear-topicos.sh. Declararlos aqui con
 * NewTopic garantiza que existan aunque el servicio apunte a un cluster nuevo; si ya existen,
 * KafkaAdmin no los modifica (la operacion es idempotente).
 */
@Configuration
@EnableConfigurationProperties(KafkaTopicosProperties.class)
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    private final KafkaTopicosProperties props;

    public KafkaConfig(KafkaTopicosProperties props) {
        this.props = props;
    }

    // ================================================================ TOPICOS

    /** Topico que este servicio produce: estado del inventario (compactado por productoId). */
    @Bean
    public KafkaAdmin.NewTopics topicosCatalogo() {
        KafkaTopicosProperties.Topico inventario = props.topicos().inventarioActualizado();
        KafkaTopicosProperties.Dlt dlt = props.dlt();
        String ventas = props.topicos().ventasConfirmadas().nombre();

        return new KafkaAdmin.NewTopics(
                TopicBuilder.name(inventario.nombre())
                        .partitions(inventario.particiones())
                        .replicas(inventario.replicas())
                        .configs(inventario.configuracionesOVacio())
                        .build(),
                // DLT del topico que este servicio CONSUME: aqui llegan las ventas que fallaron
                TopicBuilder.name(dlt.nombrePara(ventas))
                        .partitions(dlt.particiones())
                        .replicas(dlt.replicas())
                        .configs(dlt.configuracionesOVacio())
                        .build());
    }

    // ======================================================= MANEJO DE ERRORES

    /**
     * Manejo de errores de todos los @KafkaListener del servicio (Spring Boot lo asigna
     * automaticamente a la fabrica de listeners):
     *
     * <pre>
     *  Error                                              Reintentos           Destino
     *  -------------------------------------------------  -------------------  ------------------
     *  JSON que no se puede deserializar                  no                   DLT
     *  Mensaje invalido / regla de negocio                no                   DLT
     *  Recuperable (BD caida, bloqueo optimista, red)     3 (1 s, 2 s, 4 s)    DLT si persiste
     * </pre>
     *
     * Despues de enviar al DLT el offset se confirma: un mensaje defectuoso nunca bloquea la particion.
     */
    @Bean
    public CommonErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> kafkaTemplate,
                                                ProducerFactory<Object, Object> producerFactory) {
        KafkaTopicosProperties.Reintentos cfg = props.reintentos();

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(cfg.maxIntentos());
        backOff.setInitialInterval(cfg.intervaloInicialMs());
        backOff.setMultiplier(cfg.multiplicador());
        backOff.setMaxInterval(cfg.intervaloMaximoMs());

        DefaultErrorHandler handler = new DefaultErrorHandler(recuperadorConLog(kafkaTemplate, producerFactory), backOff);
        // DeserializationException y otras de Spring Kafka ya vienen como no reintentables
        handler.addNotRetryableExceptions(
                MensajeInvalidoException.class,
                ReglaNegocioException.class,
                IllegalArgumentException.class);
        handler.setRetryListeners((registro, error, intento) ->
                log.warn("[KAFKA-REINTENTO] Intento {}/{} fallido para {}-{}@{} (key={}): {}",
                        intento, cfg.maxIntentos() + 1, registro.topic(), registro.partition(),
                        registro.offset(), registro.key(), causaRaiz(error).getMessage()));
        return handler;
    }

    /**
     * Envia el mensaje fallido a {@code <topico>.DLT} con cabeceras de diagnostico
     * (kafka_dlt-exception-message, kafka_dlt-original-offset, ...) y lo deja registrado en el log.
     * Si el valor nunca se pudo deserializar, se reenvian los bytes originales tal cual.
     */
    private ConsumerRecordRecoverer recuperadorConLog(KafkaTemplate<Object, Object> kafkaTemplate,
                                                     ProducerFactory<Object, Object> producerFactory) {
        Map<Class<?>, KafkaOperations<?, ?>> plantillas = new LinkedHashMap<>();
        plantillas.put(byte[].class, plantillaBytes(producerFactory));
        plantillas.put(Object.class, kafkaTemplate);

        DeadLetterPublishingRecoverer dlt = new DeadLetterPublishingRecoverer(plantillas,
                // particion -1: el DLT tiene 1 sola particion, Kafka elige donde escribir
                (registro, error) -> new TopicPartition(props.dlt().nombrePara(registro.topic()), -1));

        return (registro, error) -> {
            log.error("[KAFKA-DLT] Mensaje enviado a {} | origen {}-{}@{} | key={} | causa: {} | valor: {}",
                    props.dlt().nombrePara(registro.topic()), registro.topic(), registro.partition(),
                    registro.offset(), registro.key(), causaRaiz(error).getMessage(), registro.value());
            dlt.accept(registro, error);
        };
    }

    /** Template que escribe bytes crudos: para reenviar al DLT un JSON que no se pudo leer. */
    private KafkaTemplate<Object, byte[]> plantillaBytes(ProducerFactory<Object, Object> producerFactory) {
        Map<String, Object> config = new HashMap<>(producerFactory.getConfigurationProperties());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "productos-catalogo-dlt");
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config));
    }

    static Throwable causaRaiz(Throwable error) {
        Throwable causa = error;
        while (causa.getCause() != null && causa.getCause() != causa) {
            causa = causa.getCause();
        }
        return causa;
    }
}
