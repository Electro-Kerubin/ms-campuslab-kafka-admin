package org.campuslab.kafkaadmin.dto;

import java.time.Instant;
import java.util.List;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;

/** Reinicio de offsets de un grupo sobre un tópico. Con dryRun=true solo muestra qué cambiaría. */
public record ResetOffsetsRequest(
        @Schema(example = "bookings.events") @NotBlank(message = "El tópico es obligatorio") String topic,
        @NotNull(message = "strategy es obligatorio") OffsetResetStrategy strategy,
        @Schema(description = "Solo para strategy=OFFSET") @PositiveOrZero(message = "El offset no puede ser negativo") Long offset,
        @Schema(description = "Solo para strategy=TIMESTAMP, en ISO-8601", example = "2026-11-20T00:00:00Z") Instant timestamp,
        @Schema(description = "Particiones a mover; vacío = todas") List<Integer> partitions,
        @Schema(description = "true = no aplica el cambio, solo lo calcula", defaultValue = "false") Boolean dryRun
) {

    public ResetOffsetsRequest {
        partitions = partitions == null ? List.of() : List.copyOf(partitions);
        dryRun = dryRun != null && dryRun;
    }
}
