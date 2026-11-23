package org.campuslab.kafkaadmin.service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.campuslab.kafkaadmin.dto.ConsumerGroupDetail;
import org.campuslab.kafkaadmin.dto.ConsumerGroupSummary;
import org.campuslab.kafkaadmin.dto.GroupMemberInfo;
import org.campuslab.kafkaadmin.dto.OffsetChange;
import org.campuslab.kafkaadmin.dto.PartitionLag;
import org.campuslab.kafkaadmin.dto.ResetOffsetsRequest;
import org.campuslab.kafkaadmin.dto.ResetOffsetsResponse;
import org.campuslab.kafkaadmin.dto.TopicPartitionRef;
import org.campuslab.kafkaadmin.exception.InvalidConfigurationException;
import org.campuslab.kafkaadmin.exception.ResourceConflictException;
import org.campuslab.kafkaadmin.exception.ResourceNotFoundException;
import org.campuslab.kafkaadmin.exception.UnprocessableOperationException;
import org.springframework.stereotype.Service;

/**
 * Consumer groups: listar, ver estado/miembros/lag, reiniciar offsets y eliminar.
 *
 * Lag = último offset del tópico (endOffset) - último offset confirmado por el grupo (committedOffset).
 * Cerca de 0 el grupo va al día; creciente, el productor es más rápido que los consumidores; alto y estático,
 * los consumidores están caídos o atascados.
 */
@Service
public class ConsumerGroupAdminService {

    private final Admin admin;
    private final KafkaErrorTranslator kafka;
    private final TopicAdminService topics;
    private final OffsetLookup offsets;

    public ConsumerGroupAdminService(Admin admin, KafkaErrorTranslator kafka, TopicAdminService topics, OffsetLookup offsets) {
        this.admin = admin;
        this.kafka = kafka;
        this.topics = topics;
        this.offsets = offsets;
    }

    // ---------- Consulta ----------

    public List<ConsumerGroupSummary> list() {
        return kafka.await(admin.listConsumerGroups().all(), "listar consumer groups", "clúster").stream()
                .map(listing -> new ConsumerGroupSummary(listing.groupId(), stateOf(listing)))
                .sorted(Comparator.comparing(ConsumerGroupSummary::groupId))
                .toList();
    }

    public ConsumerGroupDetail describe(String groupId) {
        ConsumerGroupDescription description = describeOrThrow(groupId);

        List<GroupMemberInfo> members = description.members().stream()
                .map(ConsumerGroupAdminService::toMember)
                .sorted(Comparator.comparing(GroupMemberInfo::memberId))
                .toList();

        Map<TopicPartition, OffsetAndMetadata> committed = committedOffsets(groupId);
        Map<TopicPartition, Long> end = offsets.latest(committed.keySet());

        // TreeMap por tópico y partición para una salida estable
        Map<TopicPartition, PartitionLag> lags = new TreeMap<>(
                Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition));
        long totalLag = 0;
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committed.entrySet()) {
            long committedOffset = entry.getValue().offset();
            long endOffset = end.getOrDefault(entry.getKey(), committedOffset);
            long lag = Math.max(0, endOffset - committedOffset);
            totalLag += lag;
            lags.put(entry.getKey(), new PartitionLag(entry.getKey().topic(), entry.getKey().partition(),
                    committedOffset, endOffset, lag));
        }

        return new ConsumerGroupDetail(groupId, description.state().name(), description.partitionAssignor(),
                description.coordinator() == null ? null : description.coordinator().id(),
                members, List.copyOf(lags.values()), totalLag);
    }

    // ---------- Reinicio de offsets ----------

    /**
     * Mueve el offset del grupo en un tópico. Kafka solo lo permite con el grupo SIN consumidores conectados
     * (si no, el consumidor activo lo volvería a sobrescribir): hay que detener las instancias antes.
     */
    public ResetOffsetsResponse resetOffsets(String groupId, ResetOffsetsRequest request) {
        validateTarget(request);
        ConsumerGroupDescription group = describeOrThrow(groupId);
        if (group.state() != ConsumerGroupState.EMPTY) {
            throw new ResourceConflictException("El consumer group '" + groupId + "' está " + group.state().name()
                    + ": detén todos sus consumidores (estado EMPTY) antes de reiniciar offsets");
        }

        TopicDescription topic = topics.describeOrThrow(request.topic());
        List<TopicPartition> targets = partitionsToMove(request, topic);

        Map<TopicPartition, Long> destination = switch (request.strategy()) {
            case EARLIEST -> offsets.earliest(targets);
            case LATEST -> offsets.latest(targets);
            case TIMESTAMP -> offsets.atTimestamp(targets, request.timestamp());
            case OFFSET -> explicitOffset(targets, request.offset());
        };

        Map<TopicPartition, OffsetAndMetadata> committed = committedOffsets(groupId);
        List<OffsetChange> changes = new ArrayList<>();
        Map<TopicPartition, OffsetAndMetadata> toApply = new HashMap<>();
        for (TopicPartition partition : targets) {
            long to = destination.get(partition);
            OffsetAndMetadata before = committed.get(partition);
            changes.add(new OffsetChange(partition.partition(), before == null ? null : before.offset(), to));
            toApply.put(partition, new OffsetAndMetadata(to));
        }
        changes.sort(Comparator.comparingInt(OffsetChange::partition));

        boolean apply = !request.dryRun();
        if (apply) {
            kafka.await(admin.alterConsumerGroupOffsets(groupId, toApply).all(),
                    "reiniciar los offsets", "consumer group '" + groupId + "'");
        }
        return new ResetOffsetsResponse(groupId, request.topic(), request.strategy(), request.dryRun(), apply, changes);
    }

    // ---------- Eliminación ----------

    /** Elimina el grupo y sus offsets. Solo si no tiene consumidores conectados. */
    public void delete(String groupId) {
        describeOrThrow(groupId);
        kafka.await(admin.deleteConsumerGroups(List.of(groupId)).all(), "eliminar el consumer group",
                "consumer group '" + groupId + "'");
    }

    // ---------- Apoyo ----------

    private ConsumerGroupDescription describeOrThrow(String groupId) {
        String subject = "consumer group '" + groupId + "'";
        ConsumerGroupDescription description = kafka.await(
                admin.describeConsumerGroups(List.of(groupId)).all(), "describir el consumer group", subject).get(groupId);
        // Kafka no falla ante un grupo desconocido: lo describe como DEAD
        if (description == null || description.state() == ConsumerGroupState.DEAD) {
            throw new ResourceNotFoundException("No existe el " + subject);
        }
        return description;
    }

    private Map<TopicPartition, OffsetAndMetadata> committedOffsets(String groupId) {
        Map<TopicPartition, OffsetAndMetadata> committed = kafka.await(
                admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata(),
                "leer los offsets del grupo", "consumer group '" + groupId + "'");
        // Un valor null significa "sin offset confirmado": no aporta al lag
        return committed.entrySet().stream()
                .filter(entry -> entry.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static void validateTarget(ResetOffsetsRequest request) {
        switch (request.strategy()) {
            case OFFSET -> {
                if (request.offset() == null) {
                    throw new InvalidConfigurationException("strategy=OFFSET requiere el campo 'offset'");
                }
            }
            case TIMESTAMP -> {
                if (request.timestamp() == null) {
                    throw new InvalidConfigurationException("strategy=TIMESTAMP requiere el campo 'timestamp' (ISO-8601)");
                }
            }
            default -> { /* EARLIEST y LATEST no llevan parámetros */ }
        }
    }

    private static List<TopicPartition> partitionsToMove(ResetOffsetsRequest request, TopicDescription topic) {
        Set<Integer> existing = topic.partitions().stream().map(p -> p.partition()).collect(Collectors.toSet());
        List<Integer> requested = request.partitions().isEmpty() ? existing.stream().sorted().toList() : request.partitions();
        for (Integer partition : requested) {
            if (!existing.contains(partition)) {
                throw new InvalidConfigurationException("El tópico '" + request.topic() + "' no tiene la partición " + partition
                        + " (tiene 0.." + (existing.size() - 1) + ")");
            }
        }
        return requested.stream().distinct().map(p -> new TopicPartition(request.topic(), p)).toList();
    }

    // El offset pedido debe caer dentro de lo que la partición aún conserva
    private Map<TopicPartition, Long> explicitOffset(List<TopicPartition> targets, long offset) {
        Map<TopicPartition, Long> first = offsets.earliest(targets);
        Map<TopicPartition, Long> last = offsets.latest(targets);
        Map<TopicPartition, Long> destination = new HashMap<>();
        for (TopicPartition partition : targets) {
            if (offset < first.get(partition) || offset > last.get(partition)) {
                throw new UnprocessableOperationException("El offset " + offset + " está fuera del rango de "
                        + partition + ": [" + first.get(partition) + ", " + last.get(partition) + "]");
            }
            destination.put(partition, offset);
        }
        return destination;
    }

    private static GroupMemberInfo toMember(MemberDescription member) {
        List<TopicPartitionRef> assigned = member.assignment().topicPartitions().stream()
                .sorted(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition))
                .map(tp -> new TopicPartitionRef(tp.topic(), tp.partition()))
                .toList();
        return new GroupMemberInfo(member.consumerId(), member.clientId(), member.host(),
                member.groupInstanceId().orElse(null), assigned);
    }

    private static String stateOf(ConsumerGroupListing listing) {
        return listing.state().map(Enum::name).orElse("UNKNOWN");
    }
}
