package org.campuslab.kafkaadmin.exception;

/** El clúster rechazó la operación por una razón no prevista. Se responde 502. */
public class BrokerOperationException extends RuntimeException {

    public BrokerOperationException(String message, Throwable cause) {
        super(message, cause);
    }
}
