package com.pedidos360.catalogo.messaging.kafka;

import com.pedidos360.catalogo.config.KafkaTopicosProperties;
import com.pedidos360.catalogo.dto.kafka.InventarioActualizadoEvent;
import com.pedidos360.catalogo.dto.kafka.TipoEventoInventario;
import com.pedidos360.catalogo.messaging.kafka.inventario.KafkaInventarioEventPublisher;
import com.pedidos360.catalogo.model.Producto;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Productor del caso de uso 2: topico, key, momento del envio y tolerancia a fallas de Kafka. */
class KafkaInventarioEventPublisherTest {

    private static final String TOPICO = "pedidos360.inventario.actualizado";

    private KafkaTemplate<String, Object> kafkaTemplate;
    private KafkaInventarioEventPublisher publisher;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        kafkaTemplate = mock(KafkaTemplate.class);
        KafkaTopicosProperties props = mock(KafkaTopicosProperties.class, RETURNS_DEEP_STUBS);
        when(props.topicos().inventarioActualizado().nombre()).thenReturn(TOPICO);
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(new CompletableFuture<SendResult<String, Object>>());
        publisher = new KafkaInventarioEventPublisher(kafkaTemplate, props);
    }

    @AfterEach
    void limpiarTransaccion() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    private static InventarioActualizadoEvent stockDescontado() {
        Producto doble = new Producto("Hamburguesa Doble", "Doble carne", 8990.0, 0, "Hamburguesas");
        doble.setId(3L);
        return InventarioActualizadoEvent.desde(doble, TipoEventoInventario.STOCK_DESCONTADO, 6, "PED-1");
    }

    @Test
    void publicaElEstadoCompletoConKeyIgualAlProductoId() {
        InventarioActualizadoEvent evento = stockDescontado();

        publisher.publicar(evento);

        // key = productoId: orden por producto y requisito de la compactacion
        verify(kafkaTemplate).send(TOPICO, "3", evento);
    }

    @Test
    void dentroDeUnaTransaccionEsperaAlCommitParaPublicar() {
        TransactionSynchronizationManager.initSynchronization();
        InventarioActualizadoEvent evento = stockDescontado();

        publisher.publicar(evento);
        verify(kafkaTemplate, never()).send(anyString(), anyString(), any());

        // Simula el commit: recien ahi se publica (un rollback nunca se anuncia)
        TransactionSynchronizationManager.getSynchronizations().forEach(TransactionSynchronization::afterCommit);
        verify(kafkaTemplate).send(TOPICO, "3", evento);
    }

    @Test
    void laEliminacionSePublicaComoTombstone() {
        publisher.publicarEliminacion(7L);

        verify(kafkaTemplate).send(eq(TOPICO), eq("7"), isNull());
    }

    @Test
    void siKafkaNoEstaDisponibleLaOperacionDeNegocioNoFalla() {
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenThrow(new org.apache.kafka.common.errors.TimeoutException("sin brokers"));

        assertThatCode(() -> publisher.publicar(stockDescontado())).doesNotThrowAnyException();
    }
}
