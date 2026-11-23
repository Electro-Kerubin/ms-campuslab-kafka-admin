package org.campuslab.kafkaadmin.service;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

/** Consulta los offsets de las particiones (primero, último, o el de un instante). Lo usan tópicos y consumer groups. */
@Component
public class OffsetLookup {

    private final Admin admin;
    private final KafkaErrorTranslator kafka;

    public OffsetLookup(Admin admin, KafkaErrorTranslator kafka) {
        this.admin = admin;
        this.kafka = kafka;
    }

    /** Offset del primer mensaje que aún conserva cada partición (sube cuando la retención borra segmentos). */
    public Map<TopicPartition, Long> earliest(Collection<TopicPartition> partitions) {
        return lookup(partitions, OffsetSpec.earliest());
    }

    /** Offset que tendrá el próximo mensaje de cada partición (= cantidad de mensajes escritos en toda su historia). */
    public Map<TopicPartition, Long> latest(Collection<TopicPartition> partitions) {
        return lookup(partitions, OffsetSpec.latest());
    }

    /** Offset del primer mensaje producido en o después del instante; si no hay ninguno, el final de la partición. */
    public Map<TopicPartition, Long> atTimestamp(Collection<TopicPartition> partitions, Instant instant) {
        Map<TopicPartition, Long> found = lookup(partitions, OffsetSpec.forTimestamp(instant.toEpochMilli()));
        Map<TopicPartition, Long> end = null;
        Map<TopicPartition, Long> result = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : found.entrySet()) {
            if (entry.getValue() < 0) {          // Kafka devuelve -1 cuando ningún mensaje es tan reciente
                if (end == null) end = latest(partitions);
                result.put(entry.getKey(), end.get(entry.getKey()));
            } else {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    private Map<TopicPartition, Long> lookup(Collection<TopicPartition> partitions, OffsetSpec spec) {
        if (partitions.isEmpty()) {
            return Map.of();
        }
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        partitions.forEach(partition -> request.put(partition, spec));
        Map<TopicPartition, ListOffsetsResultInfo> response =
                kafka.await(admin.listOffsets(request).all(), "consultar offsets", "tópico");
        Map<TopicPartition, Long> offsets = new HashMap<>();
        response.forEach((partition, info) -> offsets.put(partition, info.offset()));
        return offsets;
    }
}
