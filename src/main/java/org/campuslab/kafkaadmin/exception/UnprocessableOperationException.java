package org.campuslab.kafkaadmin.exception;

/**
 * La petición está bien formada pero el clúster no puede cumplirla: más réplicas que brokers, reducir particiones
 * (Kafka solo permite aumentarlas), borrar un tópico interno... Se responde 422.
 */
public class UnprocessableOperationException extends RuntimeException {

    public UnprocessableOperationException(String message) {
        super(message);
    }
}
