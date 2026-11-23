package org.campuslab.kafkaadmin.controller;

import java.util.List;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.campuslab.kafkaadmin.dto.ConsumerGroupDetail;
import org.campuslab.kafkaadmin.dto.ConsumerGroupSummary;
import org.campuslab.kafkaadmin.dto.NamePatterns;
import org.campuslab.kafkaadmin.dto.ResetOffsetsRequest;
import org.campuslab.kafkaadmin.dto.ResetOffsetsResponse;
import org.campuslab.kafkaadmin.service.ConsumerGroupAdminService;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Recurso "consumer groups".
 *
 * <pre>
 *  GET    /api/kafka/consumer-groups                      lista los grupos y su estado
 *  GET    /api/kafka/consumer-groups/{groupId}            miembros, particiones asignadas y lag
 *  POST   /api/kafka/consumer-groups/{groupId}/offsets    reinicia los offsets (el grupo debe estar sin consumidores)
 *  DELETE /api/kafka/consumer-groups/{groupId}            elimina el grupo (debe estar sin consumidores)
 * </pre>
 */
@RestController
@RequestMapping("/api/kafka/consumer-groups")
@PreAuthorize("hasRole('ADMIN')")
@Tag(name = "Consumer groups", description = "Estado, lag y reinicio de offsets de los grupos de consumidores")
public class ConsumerGroupController {

    private static final String INVALID_GROUP = "Identificador de grupo inválido";

    private final ConsumerGroupAdminService groups;

    public ConsumerGroupController(ConsumerGroupAdminService groups) {
        this.groups = groups;
    }

    @GetMapping
    @Operation(summary = "Listar consumer groups")
    @ApiResponse(responseCode = "200", description = "Cada grupo con su estado (STABLE, EMPTY, PREPARING_REBALANCE...)")
    public List<ConsumerGroupSummary> list() {
        return groups.list();
    }

    @GetMapping("/{groupId}")
    @Operation(summary = "Describir un consumer group",
            description = "Miembros con sus particiones asignadas y, por partición, offset confirmado, último offset y lag.")
    @ApiResponse(responseCode = "200", description = "Detalle del grupo con el lag total")
    @ApiResponse(responseCode = "404", description = "El grupo no existe")
    public ConsumerGroupDetail describe(@PathVariable @Size(max = NamePatterns.GROUP_MAX_LENGTH)
                                        @Pattern(regexp = NamePatterns.GROUP_ID, message = INVALID_GROUP) String groupId) {
        return groups.describe(groupId);
    }

    @PostMapping("/{groupId}/offsets")
    @Operation(summary = "Reiniciar los offsets de un grupo",
            description = "Mueve el grupo al principio (EARLIEST), al final (LATEST), a un offset (OFFSET) o a un instante (TIMESTAMP). "
                    + "El grupo debe estar EMPTY: detén antes sus consumidores. Con dryRun=true solo muestra qué cambiaría.")
    @ApiResponse(responseCode = "200", description = "Movimiento por partición (applied=false si fue dryRun)")
    @ApiResponse(responseCode = "400", description = "Falta offset/timestamp, o la partición no existe")
    @ApiResponse(responseCode = "404", description = "El grupo o el tópico no existe")
    @ApiResponse(responseCode = "409", description = "El grupo tiene consumidores conectados")
    @ApiResponse(responseCode = "422", description = "El offset está fuera del rango que conserva la partición")
    public ResetOffsetsResponse resetOffsets(@PathVariable @Size(max = NamePatterns.GROUP_MAX_LENGTH)
                                             @Pattern(regexp = NamePatterns.GROUP_ID, message = INVALID_GROUP) String groupId,
                                             @Valid @RequestBody ResetOffsetsRequest request) {
        return groups.resetOffsets(groupId, request);
    }

    @DeleteMapping("/{groupId}")
    @Operation(summary = "Eliminar un consumer group", description = "Borra el grupo y sus offsets. Solo si no tiene consumidores conectados.")
    @ApiResponse(responseCode = "204", description = "Grupo eliminado")
    @ApiResponse(responseCode = "404", description = "El grupo no existe")
    @ApiResponse(responseCode = "409", description = "El grupo tiene consumidores conectados")
    public ResponseEntity<Void> delete(@PathVariable @Size(max = NamePatterns.GROUP_MAX_LENGTH)
                                       @Pattern(regexp = NamePatterns.GROUP_ID, message = INVALID_GROUP) String groupId) {
        groups.delete(groupId);
        return ResponseEntity.noContent().build();
    }
}
