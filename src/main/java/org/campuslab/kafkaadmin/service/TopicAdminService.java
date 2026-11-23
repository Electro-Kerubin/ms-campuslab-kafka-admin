package org.campuslab.kafkaadmin.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.campuslab.kafkaadmin.dto.ConfigValue;
import org.campuslab.kafkaadmin.dto.PartitionInfo;
import org.campuslab.kafkaadmin.dto.TopicConfigUpdateRequest;
import org.campuslab.kafkaadmin.dto.TopicDetailResponse;
import org.campuslab.kafkaadmin.dto.TopicRequest;
import org.campuslab.kafkaadmin.dto.TopicResponse;
import org.campuslab.kafkaadmin.dto.TopicSummary;
import org.campuslab.kafkaadmin.exception.InvalidConfigurationException;
import org.campuslab.kafkaadmin.exception.UnprocessableOperationException;
import org.springframework.stereotype.Service;

/**
 * Administración de tópicos: crear, listar, describir, modificar configuración, aumentar particiones y eliminar.
 * Es el único servicio que habla con el AdminClient para tópicos; los controladores no conocen Kafka.
 */
@Service
public class TopicAdminService {

    private final Admin admin;
    private final KafkaErrorTranslator kafka;
    private final OffsetLookup offsets;

    public TopicAdminService(Admin admin, KafkaErrorTranslator kafka, OffsetLookup offsets) {
        this.admin = admin;
        this.kafka = kafka;
        this.offsets = offsets;
    }

    // ---------- Consulta ----------

    public List<TopicSummary> list(boolean includeInternal) {
        Set<String> names = kafka.await(
                admin.listTopics(new ListTopicsOptions().listInternal(includeInternal)).names(), "listar tópicos", "clúster");
        if (names.isEmpty()) {
            return List.of();
        }
        Map<String, TopicDescription> descriptions = kafka.await(
                admin.describeTopics(names).allTopicNames(), "describir tópicos", "clúster");
        return descriptions.values().stream()
                .map(d -> new TopicSummary(d.name(), d.partitions().size(), replicationFactorOf(d), d.isInternal()))
                .sorted(Comparator.comparing(TopicSummary::name))
                .toList();
    }

    public TopicDetailResponse describe(String name) {
        TopicDescription description = describeOrThrow(name);

        List<TopicPartition> partitions = description.partitions().stream()
                .map(p -> new TopicPartition(name, p.partition())).toList();
        Map<TopicPartition, Long> earliest = offsets.earliest(partitions);
        Map<TopicPartition, Long> latest = offsets.latest(partitions);

        List<PartitionInfo> partitionInfos = new ArrayList<>();
        long total = 0;
        for (TopicPartitionInfo p : description.partitions()) {
            TopicPartition tp = new TopicPartition(name, p.partition());
            long start = earliest.getOrDefault(tp, 0L);
            long end = latest.getOrDefault(tp, 0L);
            total += end - start;
            partitionInfos.add(new PartitionInfo(
                    p.partition(),
                    p.leader() == null ? null : p.leader().id(),
                    p.replicas().stream().map(Node::id).toList(),
                    p.isr().stream().map(Node::id).toList(),
                    start, end, end - start));
        }
        partitionInfos.sort(Comparator.comparingInt(PartitionInfo::partition));

        return new TopicDetailResponse(name, description.isInternal(), replicationFactorOf(description),
                total, partitionInfos, configsOf(name));
    }

    // ---------- Creación ----------

    public TopicResponse create(TopicRequest request) {
        TopicConfigRules.validate(request.configs());
        int brokers = brokerCount();
        if (request.replicationFactor() > brokers) {
            throw new UnprocessableOperationException("El factor de replicación (" + request.replicationFactor()
                    + ") no puede superar la cantidad de brokers del clúster (" + brokers + ")");
        }
        checkMinIsr(request.configs(), request.replicationFactor());

        NewTopic topic = new NewTopic(request.name(), request.partitions(), request.replicationFactor())
                .configs(request.configs());
        kafka.await(admin.createTopics(List.of(topic)).all(), "crear el tópico", "tópico '" + request.name() + "'");
        return new TopicResponse(request.name(), request.partitions(), request.replicationFactor(), request.configs());
    }

    // ---------- Modificación ----------

    /** Fija y/o devuelve a su valor por defecto configuraciones del tópico. Devuelve el tópico ya actualizado. */
    public TopicDetailResponse updateConfig(String name, TopicConfigUpdateRequest request) {
        if (request.set().isEmpty() && request.reset().isEmpty()) {
            throw new InvalidConfigurationException("Indica al menos una configuración en 'set' o en 'reset'");
        }
        TopicConfigRules.validate(request.set());
        request.reset().forEach(TopicConfigRules::validateKey);
        request.reset().stream().filter(request.set()::containsKey).findFirst().ifPresent(key -> {
            throw new InvalidConfigurationException("La configuración '" + key + "' está en 'set' y en 'reset' a la vez");
        });

        TopicDescription current = describeOrThrow(name);
        checkMinIsr(request.set(), replicationFactorOf(current));

        List<AlterConfigOp> operations = new ArrayList<>();
        request.set().forEach((key, value) ->
                operations.add(new AlterConfigOp(new ConfigEntry(key, value.trim()), AlterConfigOp.OpType.SET)));
        request.reset().forEach(key ->
                operations.add(new AlterConfigOp(new ConfigEntry(key, null), AlterConfigOp.OpType.DELETE)));

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, name);
        kafka.await(admin.incrementalAlterConfigs(Map.of(resource, operations)).all(),
                "modificar la configuración", "tópico '" + name + "'");
        return describe(name);
    }

    /**
     * Sube el total de particiones. Kafka no permite reducirlas. Aumentarlas cambia a qué partición va cada clave
     * (hash(clave) % particiones): los mensajes nuevos de una misma clave pueden dejar de caer donde caían los viejos.
     */
    public TopicDetailResponse increasePartitions(String name, int partitions) {
        int current = describeOrThrow(name).partitions().size();
        if (partitions == current) {
            throw new UnprocessableOperationException("El tópico '" + name + "' ya tiene " + current + " particiones");
        }
        if (partitions < current) {
            throw new UnprocessableOperationException("Kafka no permite reducir particiones: '" + name + "' tiene "
                    + current + " y se pidieron " + partitions);
        }
        kafka.await(admin.createPartitions(Map.of(name, NewPartitions.increaseTo(partitions))).all(),
                "aumentar las particiones", "tópico '" + name + "'");
        return describe(name);
    }

    // ---------- Eliminación ----------

    public void delete(String name) {
        if (name.startsWith("__")) {
            throw new UnprocessableOperationException("'" + name + "' es un tópico interno de Kafka y no se puede eliminar");
        }
        describeOrThrow(name);   // 404 con mensaje claro si no existe
        kafka.await(admin.deleteTopics(List.of(name)).all(), "eliminar el tópico", "tópico '" + name + "'");
    }

    // ---------- Apoyo (también lo usa el aprovisionamiento) ----------

    TopicDescription describeOrThrow(String name) {
        Map<String, TopicDescription> found = kafka.await(
                admin.describeTopics(List.of(name)).allTopicNames(), "describir el tópico", "tópico '" + name + "'");
        return found.get(name);
    }

    /** Configuración efectiva (incluye lo heredado del broker) con el origen de cada valor. */
    Map<String, ConfigValue> configsOf(String name) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, name);
        Map<ConfigResource, Config> configs = kafka.await(
                admin.describeConfigs(List.of(resource)).all(), "leer la configuración", "tópico '" + name + "'");
        Map<String, ConfigValue> result = new LinkedHashMap<>();
        configs.get(resource).entries().stream()
                .sorted(Comparator.comparing(ConfigEntry::name))
                .forEach(entry -> result.put(entry.name(),
                        new ConfigValue(entry.value(), entry.source().name(), entry.isReadOnly())));
        return result;
    }

    int brokerCount() {
        Collection<Node> nodes = kafka.await(admin.describeCluster().nodes(), "consultar el clúster", "clúster");
        return nodes.size();
    }

    static int replicationFactorOf(TopicDescription description) {
        return description.partitions().isEmpty() ? 0 : description.partitions().get(0).replicas().size();
    }

    // Con min.insync.replicas > réplicas, acks=all dejaría de funcionar: ningún productor podría escribir
    private static void checkMinIsr(Map<String, String> configs, int replicationFactor) {
        String minIsr = configs.get("min.insync.replicas");
        if (minIsr != null && Long.parseLong(minIsr.trim()) > replicationFactor) {
            throw new UnprocessableOperationException("min.insync.replicas (" + minIsr.trim()
                    + ") no puede superar el factor de replicación (" + replicationFactor + "): ningún productor con acks=all podría escribir");
        }
    }
}
