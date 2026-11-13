package com.pedidos360.catalogo.config;

import com.pedidos360.catalogo.exception.ReglaNegocioException;
import com.pedidos360.catalogo.messaging.comun.MensajeInvalidoException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.classify.BinaryExceptionClassifier;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Politica de errores de los consumidores: los mensajes defectuosos van al DLT sin reintentos
 * (no bloquean la particion) y los errores transitorios se reintentan con backoff.
 */
class KafkaErrorHandlerTest {

    private BinaryExceptionClassifier clasificador;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        KafkaTopicosProperties props = mock(KafkaTopicosProperties.class, RETURNS_DEEP_STUBS);
        when(props.reintentos().maxIntentos()).thenReturn(3);
        when(props.reintentos().intervaloInicialMs()).thenReturn(1000L);
        when(props.reintentos().multiplicador()).thenReturn(2.0);
        when(props.reintentos().intervaloMaximoMs()).thenReturn(8000L);
        ProducerFactory<Object, Object> producerFactory = mock(ProducerFactory.class);
        when(producerFactory.getConfigurationProperties()).thenReturn(Map.of());

        CommonErrorHandler handler = new KafkaConfig(props)
                .kafkaErrorHandler(mock(KafkaTemplate.class), producerFactory);
        clasificador = ReflectionTestUtils.invokeMethod(handler, "getClassifier");
    }

    private boolean seReintenta(Throwable error) {
        return clasificador.classify(error);
    }

    @Test
    void losMensajesInvalidosVanDirectoAlDlt() {
        assertThat(seReintenta(new MensajeInvalidoException("JSON incompleto"))).isFalse();
        assertThat(seReintenta(new ReglaNegocioException("total incorrecto"))).isFalse();
        assertThat(seReintenta(new DeserializationException("no es JSON", new byte[0], false, null))).isFalse();
    }

    @Test
    void losErroresTransitoriosSeReintentan() {
        assertThat(seReintenta(new TransientDataAccessResourceException("MySQL no responde"))).isTrue();
        assertThat(seReintenta(new RuntimeException("falla de red"))).isTrue();
    }
}
