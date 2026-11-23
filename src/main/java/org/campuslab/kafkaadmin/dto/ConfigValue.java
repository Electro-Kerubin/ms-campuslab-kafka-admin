package org.campuslab.kafkaadmin.dto;

/**
 * Valor efectivo de una configuración de tópico y de dónde viene:
 * DYNAMIC_TOPIC_CONFIG (fijada en el tópico), STATIC_BROKER_CONFIG / DEFAULT_CONFIG (heredada del broker).
 */
public record ConfigValue(String value, String source, boolean readOnly) {
}
