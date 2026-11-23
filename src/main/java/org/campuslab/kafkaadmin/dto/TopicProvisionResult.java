package org.campuslab.kafkaadmin.dto;

import java.util.List;

/**
 * Qué pasó con un tópico al aprovisionar: CREATED (no existía), UPDATED (se alineó configuración o particiones)
 * o UNCHANGED (ya estaba como se pidió). "notes" explica cada cambio; "warnings" lo que no se pudo alinear
 * (reducir particiones o cambiar el factor de replicación no es posible en Kafka).
 */
public record TopicProvisionResult(String topic, String action, List<String> notes, List<String> warnings) {
}
