package org.campuslab.kafkaadmin.exception;

/** Una clave de configuración no permitida o un valor fuera de rango. Se responde 400. */
public class InvalidConfigurationException extends RuntimeException {

    public InvalidConfigurationException(String message) {
        super(message);
    }
}
