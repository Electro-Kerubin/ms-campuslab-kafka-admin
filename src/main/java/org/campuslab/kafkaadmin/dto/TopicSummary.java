package org.campuslab.kafkaadmin.dto;

/** Una fila del listado de tópicos. */
public record TopicSummary(String name, int partitions, int replicationFactor, boolean internal) {
}
