package org.campuslab.kafkaadmin.exception;

/** El tópico ya existe, o el grupo está activo y no admite la operación pedida. Se responde 409. */
public class ResourceConflictException extends RuntimeException {

    public ResourceConflictException(String message) {
        super(message);
    }
}
