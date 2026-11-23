package org.campuslab.kafkaadmin.dto;

/** Una fila del listado de grupos: STABLE (con consumidores), EMPTY (sin consumidores), PREPARING_REBALANCE... */
public record ConsumerGroupSummary(String groupId, String state) {
}
