package org.campuslab.kafkaadmin.dto;

import java.util.List;

public record ResetOffsetsResponse(
        String groupId,
        String topic,
        OffsetResetStrategy strategy,
        boolean dryRun,
        boolean applied,
        List<OffsetChange> changes
) {
}
