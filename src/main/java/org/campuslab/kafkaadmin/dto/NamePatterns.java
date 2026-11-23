package org.campuslab.kafkaadmin.dto;

/** Expresiones regulares de validación de nombres, compartidas por los DTO y los controladores. */
public final class NamePatterns {

    private NamePatterns() {
    }

    /** Un tópico solo puede usar letras, números, punto, guion y guion bajo (regla de Kafka); máximo 249 caracteres. */
    public static final String TOPIC_NAME = "^[A-Za-z0-9._-]+$";

    public static final int TOPIC_MAX_LENGTH = 249;

    /** Un group.id admite más símbolos que un tópico, pero nunca espacios ni "/". */
    public static final String GROUP_ID = "^[A-Za-z0-9._:@-]+$";

    public static final int GROUP_MAX_LENGTH = 255;
}
