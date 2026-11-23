package org.campuslab.kafkaadmin.dto;

/** Movimiento de una partición: "from" es null si el grupo nunca había confirmado nada ahí. */
public record OffsetChange(int partition, Long from, long to) {
}
