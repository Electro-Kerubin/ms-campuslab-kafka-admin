package org.campuslab.kafkaadmin.dto;

import java.time.Instant;
import java.util.Map;

/** Un mensaje leído para inspección (en las DLT los headers explican por qué falló). */
public record MessageView(
        int partition,
        long offset,
        Instant timestamp,
        String key,
        String value,
        Map<String, String> headers
) {
}
