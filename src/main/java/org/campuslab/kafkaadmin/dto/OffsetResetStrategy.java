package org.campuslab.kafkaadmin.dto;

/** A dónde se mueve el offset del grupo. */
public enum OffsetResetStrategy {
    /** Al primer mensaje disponible: el grupo vuelve a leer todo lo que el tópico aún conserva. */
    EARLIEST,
    /** Al final: el grupo se salta lo pendiente. */
    LATEST,
    /** A un offset concreto (campo offset). */
    OFFSET,
    /** Al primer mensaje producido en o después de un instante (campo timestamp). */
    TIMESTAMP
}
