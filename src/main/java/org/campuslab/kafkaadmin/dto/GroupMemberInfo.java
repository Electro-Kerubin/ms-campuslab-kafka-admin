package org.campuslab.kafkaadmin.dto;

import java.util.List;

/** Un consumidor del grupo y las particiones que tiene asignadas. */
public record GroupMemberInfo(
        String memberId,
        String clientId,
        String host,
        String groupInstanceId,
        List<TopicPartitionRef> assignedPartitions
) {
}
