package org.campuslab.kafkaadmin.dto;

import java.util.Map;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/**
 * Datos para crear un tópico. Solo el nombre es obligatorio: sin más datos se crea con 3 particiones y
 * factor de replicación 3 (la topología del proyecto).
 */
public record TopicRequest(
        @Schema(example = "bookings.events")
        @NotBlank(message = "El nombre del tópico es obligatorio")
        @Size(max = NamePatterns.TOPIC_MAX_LENGTH, message = "El nombre admite hasta 249 caracteres")
        @Pattern(regexp = NamePatterns.TOPIC_NAME, message = "Solo letras, números, punto, guion y guion bajo")
        String name,

        @Schema(description = "Particiones = paralelismo máximo de un consumer group", example = "3", defaultValue = "3")
        @Min(value = 1, message = "Debe haber al menos 1 partición")
        @Max(value = 1000, message = "Máximo 1000 particiones")
        Integer partitions,

        @Schema(description = "Copias de cada partición; no puede superar la cantidad de brokers", example = "3", defaultValue = "3")
        @Min(value = 1, message = "El factor de replicación mínimo es 1")
        @Max(value = 10, message = "El factor de replicación máximo es 10")
        Short replicationFactor,

        @Schema(description = "Configuración del tópico: cleanup.policy, retention.ms, retention.bytes, min.insync.replicas...",
                example = "{\"cleanup.policy\":\"delete\",\"retention.ms\":\"604800000\"}")
        Map<String, String> configs
) {

    public static final int DEFAULT_PARTITIONS = 3;
    public static final short DEFAULT_REPLICATION_FACTOR = 3;

    public TopicRequest {
        partitions = partitions == null ? DEFAULT_PARTITIONS : partitions;
        replicationFactor = replicationFactor == null ? DEFAULT_REPLICATION_FACTOR : replicationFactor;
        configs = configs == null ? Map.of() : Map.copyOf(configs);
    }
}
