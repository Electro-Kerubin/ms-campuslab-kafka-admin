package org.campuslab.kafkaadmin.exception;

/** El clúster no responde (brokers caídos, red, timeout). Se responde 503. */
public class BrokerUnavailableException extends RuntimeException {

    public BrokerUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
