package org.campuslab.kafkaadmin.dto;

import java.util.List;

/** Descripción de un grupo: estado, coordinador, miembros y lag por partición. */
public record ConsumerGroupDetail(
        String groupId,
        String state,
        String partitionAssignor,
        Integer coordinatorId,
        List<GroupMemberInfo> members,
        List<PartitionLag> offsets,
        long totalLag
) {
}
