package org.campuslab.kafkaadmin.dto;

import java.util.List;
import java.util.Map;

import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Cambios de configuración de un tópico. Al menos uno de los dos campos debe traer algo.
 * "set" fija valores; "reset" devuelve una clave al valor por defecto del broker.
 */
public record TopicConfigUpdateRequest(
        @Schema(example = "{\"retention.ms\":\"86400000\"}") Map<String, String> set,
        @Schema(example = "[\"retention.bytes\"]") List<String> reset
) {

    public TopicConfigUpdateRequest {
        set = set == null ? Map.of() : Map.copyOf(set);
        reset = reset == null ? List.of() : List.copyOf(reset);
    }
}
