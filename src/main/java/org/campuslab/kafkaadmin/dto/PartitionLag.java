package org.campuslab.kafkaadmin.dto;

/** Progreso del grupo en una partición. lag = endOffset - committedOffset (0 = al día). */
public record PartitionLag(String topic, int partition, long committedOffset, long endOffset, long lag) {
}
