package org.campuslab.kafkaadmin.config;

import java.util.List;
import java.util.Map;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * Topología de tópicos que el servicio sabe aprovisionar (prefijo campuslab.kafka-admin).
 * Vive en application.yml para que sea la misma en todos los entornos y se pueda replicar con un solo POST.
 */
@Validated
@ConfigurationProperties(prefix = "campuslab.kafka-admin")
public record ManagedTopicsProperties(
        @DefaultValue("true") boolean adaptReplicationToCluster,
        @Valid @DefaultValue List<ManagedTopic> topics
) {

    public record ManagedTopic(
            @NotBlank String name,
            @Min(1) @Max(1000) int partitions,
            @Min(1) @Max(10) short replicationFactor,
            @DefaultValue Map<String, String> configs
    ) {
    }
}
