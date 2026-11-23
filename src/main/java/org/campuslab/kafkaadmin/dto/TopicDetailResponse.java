package org.campuslab.kafkaadmin.dto;

import java.util.List;
import java.util.Map;

/** Descripción completa de un tópico: particiones (líder, réplicas, ISR, offsets) y configuración efectiva. */
public record TopicDetailResponse(
        String name,
        boolean internal,
        int replicationFactor,
        long totalMessages,
        List<PartitionInfo> partitions,
        Map<String, ConfigValue> configs
) {
}
