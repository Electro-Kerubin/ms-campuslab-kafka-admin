package org.campuslab.kafkaadmin.config;

import java.util.HashMap;
import java.util.Map;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * Cómo se conecta este servicio al clúster (prefijo campuslab.kafka). Es el único lugar donde se definen
 * los brokers: tanto el AdminClient como el consumidor que inspecciona mensajes los leen de aquí.
 */
@Validated
@ConfigurationProperties(prefix = "campuslab.kafka")
public record KafkaConnectionProperties(
        @NotBlank @DefaultValue("localhost:9092") String bootstrapServers,
        @Positive @DefaultValue("8000") int requestTimeoutMs,
        @DefaultValue Map<String, String> properties
) {

    /** Propiedades comunes a todos los clientes (admin y consumidor): brokers, timeouts y los extras de seguridad. */
    public Map<String, Object> clientProperties() {
        Map<String, Object> config = new HashMap<>(properties);
        config.put("bootstrap.servers", bootstrapServers);
        config.put("request.timeout.ms", requestTimeoutMs);
        // Sin esto, cuando no hay brokers el cliente reintenta 2 minutos antes de rendirse
        config.put("default.api.timeout.ms", requestTimeoutMs);
        config.put("client.id", "ms-campuslab-kafka-admin");
        return config;
    }
}
