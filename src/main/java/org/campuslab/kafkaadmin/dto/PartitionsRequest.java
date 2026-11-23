package org.campuslab.kafkaadmin.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

/** Nuevo total de particiones. Kafka solo permite aumentarlas, nunca reducirlas. */
public record PartitionsRequest(
        @Schema(description = "Total de particiones que debe tener el tópico", example = "6")
        @NotNull(message = "partitions es obligatorio")
        @Min(value = 1, message = "Debe haber al menos 1 partición")
        @Max(value = 1000, message = "Máximo 1000 particiones")
        Integer partitions
) {
}
