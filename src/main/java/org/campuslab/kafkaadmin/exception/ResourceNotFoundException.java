package org.campuslab.kafkaadmin.exception;

/** El tópico o el consumer group no existe. Se responde 404. */
public class ResourceNotFoundException extends RuntimeException {

    public ResourceNotFoundException(String message) {
        super(message);
    }
}
