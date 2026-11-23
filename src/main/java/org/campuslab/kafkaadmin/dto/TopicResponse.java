package org.campuslab.kafkaadmin.dto;

import java.util.Map;

/** Resultado de crear un tópico: lo que quedó definido. */
public record TopicResponse(String name, int partitions, int replicationFactor, Map<String, String> configs) {
}
