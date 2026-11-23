package org.campuslab.kafkaadmin.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.campuslab.kafkaadmin.config.KafkaConnectionProperties;
import org.campuslab.kafkaadmin.exception.BrokerOperationException;
import org.campuslab.kafkaadmin.exception.BrokerUnavailableException;
import org.campuslab.kafkaadmin.exception.InvalidConfigurationException;
import org.campuslab.kafkaadmin.exception.ResourceConflictException;
import org.campuslab.kafkaadmin.exception.ResourceNotFoundException;
import org.campuslab.kafkaadmin.exception.UnprocessableOperationException;
import org.junit.jupiter.api.Test;

/** Cada error de Kafka se convierte en la excepción de dominio (y por tanto el código HTTP) que corresponde. */
class KafkaErrorTranslatorTest {

    private final KafkaErrorTranslator translator =
            new KafkaErrorTranslator(new KafkaConnectionProperties("localhost:9092", 1000, Map.of()));

    private static KafkaFuture<String> failed(Throwable error) {
        KafkaFutureImpl<String> future = new KafkaFutureImpl<>();
        future.completeExceptionally(error);
        return future;
    }

    private void expect(Throwable kafkaError, Class<? extends RuntimeException> domain) {
        assertThatThrownBy(() -> translator.await(failed(kafkaError), "hacer algo", "tópico 'x'")).isInstanceOf(domain);
    }

    @Test
    void devuelveElResultadoCuandoTodoSaleBien() {
        assertThat(translator.await(KafkaFuture.completedFuture("ok"), "hacer algo", "tópico 'x'")).isEqualTo("ok");
    }

    @Test
    void tradueceLosErroresDelClusterAExcepcionesDeDominio() {
        expect(new UnknownTopicOrPartitionException("x"), ResourceNotFoundException.class);
        expect(new GroupIdNotFoundException("x"), ResourceNotFoundException.class);
        expect(new TopicExistsException("x"), ResourceConflictException.class);
        expect(new GroupNotEmptyException("x"), ResourceConflictException.class);
        expect(new InvalidTopicException("x"), InvalidConfigurationException.class);
        expect(new org.apache.kafka.common.errors.InvalidConfigurationException("x"), InvalidConfigurationException.class);
        expect(new InvalidReplicationFactorException("x"), UnprocessableOperationException.class);
        expect(new InvalidPartitionsException("x"), UnprocessableOperationException.class);
        expect(new TimeoutException("x"), BrokerUnavailableException.class);
        expect(new DisconnectException("x"), BrokerUnavailableException.class);
        expect(new AuthorizationException("x"), BrokerOperationException.class);
        expect(new IllegalStateException("inesperado"), BrokerOperationException.class);
    }

    @Test
    void losMensajesNombranElRecursoAfectado() {
        assertThatThrownBy(() -> translator.await(failed(new TopicExistsException("x")), "crear", "tópico 'bookings.events'"))
                .hasMessage("El tópico 'bookings.events' ya existe");
        assertThatThrownBy(() -> translator.await(failed(new UnknownTopicOrPartitionException("x")), "describir", "tópico 'nada'"))
                .hasMessage("No existe el tópico 'nada'");
        assertThatThrownBy(() -> translator.await(failed(new GroupNotEmptyException("x")), "eliminar", "consumer group 'g'"))
                .hasMessageContaining("consumer group 'g'").hasMessageContaining("consumidores activos");
    }

    @Test
    void sinRespuestaDelClusterEsUnServicioNoDisponibleYNoUnaFallaInterna() {
        KafkaFutureImpl<String> neverCompletes = new KafkaFutureImpl<>();
        // espera de seguridad = request-timeout-ms (1 s) + 2 s
        assertThatThrownBy(() -> translator.await(neverCompletes, "listar tópicos", "clúster"))
                .isInstanceOf(BrokerUnavailableException.class).hasMessageContaining("no respondió a tiempo");
    }
}
