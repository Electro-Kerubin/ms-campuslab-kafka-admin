package org.campuslab.kafkaadmin.service;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.campuslab.kafkaadmin.config.KafkaConnectionProperties;
import org.campuslab.kafkaadmin.exception.BrokerOperationException;
import org.campuslab.kafkaadmin.exception.BrokerUnavailableException;
import org.campuslab.kafkaadmin.exception.InvalidConfigurationException;
import org.campuslab.kafkaadmin.exception.ResourceConflictException;
import org.campuslab.kafkaadmin.exception.ResourceNotFoundException;
import org.campuslab.kafkaadmin.exception.UnprocessableOperationException;
import org.springframework.stereotype.Component;

/**
 * Único punto donde se espera la respuesta del clúster y se traducen los errores de Kafka a excepciones de dominio.
 * Así los servicios no conocen las excepciones del cliente y el GlobalExceptionHandler solo ve las del proyecto.
 *
 * <pre>
 *  Error de Kafka                                      Excepción de dominio              HTTP
 *  --------------------------------------------------  --------------------------------  ----
 *  UnknownTopicOrPartition / GroupIdNotFound           ResourceNotFoundException         404
 *  TopicExists / GroupNotEmpty                         ResourceConflictException         409
 *  InvalidTopic / InvalidConfiguration                 InvalidConfigurationException     400
 *  InvalidReplicationFactor / InvalidPartitions        UnprocessableOperationException   422
 *  Timeout / Disconnect / Network / LeaderNotAvailable BrokerUnavailableException        503
 *  cualquier otro                                      BrokerOperationException          502
 * </pre>
 */
@Component
public class KafkaErrorTranslator {

    private final long waitMs;

    public KafkaErrorTranslator(KafkaConnectionProperties connection) {
        // El AdminClient ya corta por su cuenta a los request-timeout-ms; esto es solo la red de seguridad
        this.waitMs = connection.requestTimeoutMs() + 2_000L;
    }

    /**
     * Espera el resultado de una operación del AdminClient.
     *
     * @param action  qué se intentaba, para el mensaje de error ("crear el tópico")
     * @param subject sobre qué recurso, para los mensajes 404/409 ("tópico 'bookings.events'")
     */
    public <T> T await(KafkaFuture<T> future, String action, String subject) {
        try {
            return future.get(waitMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new BrokerOperationException("Se interrumpió la operación: " + action, interrupted);
        } catch (TimeoutException timeout) {
            throw new BrokerUnavailableException("Kafka no respondió a tiempo al " + action, timeout);
        } catch (ExecutionException failed) {
            throw translate(failed.getCause() == null ? failed : failed.getCause(), action, subject);
        }
    }

    public RuntimeException translate(Throwable cause, String action, String subject) {
        if (cause instanceof UnknownTopicOrPartitionException || cause instanceof GroupIdNotFoundException) {
            return new ResourceNotFoundException("No existe el " + subject);
        }
        if (cause instanceof TopicExistsException) {
            return new ResourceConflictException("El " + subject + " ya existe");
        }
        if (cause instanceof GroupNotEmptyException) {
            return new ResourceConflictException("El " + subject
                    + " tiene consumidores activos: detén los consumidores antes de continuar");
        }
        if (cause instanceof InvalidTopicException
                || cause instanceof org.apache.kafka.common.errors.InvalidConfigurationException) {
            return new InvalidConfigurationException(cause.getMessage());
        }
        if (cause instanceof InvalidReplicationFactorException || cause instanceof InvalidPartitionsException) {
            return new UnprocessableOperationException(cause.getMessage());
        }
        if (cause instanceof org.apache.kafka.common.errors.TimeoutException
                || cause instanceof DisconnectException
                || cause instanceof NetworkException
                || cause instanceof LeaderNotAvailableException) {
            return new BrokerUnavailableException("No se pudo contactar al clúster Kafka al " + action, cause);
        }
        return new BrokerOperationException("Kafka rechazó la operación (" + action + "): " + cause.getMessage(), cause);
    }
}
