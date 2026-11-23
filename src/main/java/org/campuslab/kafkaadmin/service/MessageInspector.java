package org.campuslab.kafkaadmin.service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.campuslab.kafkaadmin.config.KafkaConnectionProperties;
import org.campuslab.kafkaadmin.dto.MessageView;
import org.campuslab.kafkaadmin.exception.BrokerUnavailableException;
import org.campuslab.kafkaadmin.exception.ResourceNotFoundException;
import org.springframework.stereotype.Service;

/**
 * Lee los últimos mensajes de un tópico para inspeccionarlos (típicamente una DLT: los headers
 * kafka_dlt-exception-message y kafka_dlt-original-topic explican por qué y de dónde vino cada mensaje).
 *
 * Es solo lectura y no deja rastro: el consumidor no usa group.id, así que no crea un consumer group
 * ni mueve los offsets de nadie.
 */
@Service
public class MessageInspector {

    public static final int MAX_LIMIT = 100;
    private static final int MAX_VALUE_CHARS = 4_000;
    private static final Duration POLL_BUDGET = Duration.ofSeconds(8);

    private final KafkaConnectionProperties connection;

    public MessageInspector(KafkaConnectionProperties connection) {
        this.connection = connection;
    }

    /** Los últimos {@code limit} mensajes (como máximo) de cada partición, del más reciente al más antiguo. */
    public List<MessageView> latest(String topic, int limit) {
        if (limit < 1 || limit > MAX_LIMIT) {
            throw new IllegalArgumentException("limit debe estar entre 1 y " + MAX_LIMIT);
        }

        Map<String, Object> config = new HashMap<>(connection.clientProperties());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // Inspeccionar es solo lectura: preguntar por un tópico que no existe no debe crearlo
        config.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, "ms-campuslab-kafka-admin-inspector");

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(config)) {
            List<PartitionInfo> info = consumer.partitionsFor(topic, Duration.ofMillis(connection.requestTimeoutMs()));
            if (info == null || info.isEmpty()) {
                throw new ResourceNotFoundException("No existe el tópico '" + topic + "'");
            }
            List<TopicPartition> partitions = info.stream().map(p -> new TopicPartition(topic, p.partition())).toList();
            consumer.assign(partitions);

            Map<TopicPartition, Long> start = consumer.beginningOffsets(partitions);
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
            Map<TopicPartition, Long> pending = new HashMap<>();
            for (TopicPartition partition : partitions) {
                long from = Math.max(start.get(partition), end.get(partition) - limit);
                consumer.seek(partition, from);
                if (from < end.get(partition)) {
                    pending.put(partition, end.get(partition));
                }
            }

            List<MessageView> messages = new ArrayList<>();
            long deadline = System.nanoTime() + POLL_BUDGET.toNanos();
            while (!pending.isEmpty() && System.nanoTime() < deadline) {
                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(300))) {
                    TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                    Long stop = pending.get(partition);
                    if (stop == null) {
                        continue;
                    }
                    messages.add(toView(record));
                    if (record.offset() + 1 >= stop) {
                        pending.remove(partition);
                    }
                }
            }
            return messages.stream()
                    .sorted(Comparator.comparing(MessageView::timestamp).reversed().thenComparing(MessageView::offset, Comparator.reverseOrder()))
                    .limit(limit)
                    .toList();
        } catch (org.apache.kafka.common.errors.TimeoutException timeout) {
            throw new BrokerUnavailableException("Kafka no respondió a tiempo al leer el tópico '" + topic + "'", timeout);
        }
    }

    private static MessageView toView(ConsumerRecord<byte[], byte[]> record) {
        Map<String, String> headers = new LinkedHashMap<>();
        for (Header header : record.headers()) {
            headers.put(header.key(), header.value() == null ? null : text(header.value()));
        }
        return new MessageView(record.partition(), record.offset(), Instant.ofEpochMilli(record.timestamp()),
                record.key() == null ? null : text(record.key()),
                record.value() == null ? null : text(record.value()), headers);
    }

    private static String text(byte[] bytes) {
        String value = new String(bytes, StandardCharsets.UTF_8);
        return value.length() <= MAX_VALUE_CHARS ? value : value.substring(0, MAX_VALUE_CHARS) + "…";
    }
}
