package org.campuslab.kafkaadmin.dto;

import java.util.List;

/** Estado de una partición: quién es el líder, dónde están las réplicas y cuántos mensajes retiene. */
public record PartitionInfo(
        int partition,
        Integer leader,
        List<Integer> replicas,
        List<Integer> inSyncReplicas,
        long startOffset,
        long endOffset,
        long messages
) {
}
