package org.campuslab.kafkaadmin.controller;

import java.net.URI;
import java.util.List;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;
import org.campuslab.kafkaadmin.dto.MessageView;
import org.campuslab.kafkaadmin.dto.NamePatterns;
import org.campuslab.kafkaadmin.dto.PartitionsRequest;
import org.campuslab.kafkaadmin.dto.ProvisionResponse;
import org.campuslab.kafkaadmin.dto.TopicConfigUpdateRequest;
import org.campuslab.kafkaadmin.dto.TopicDetailResponse;
import org.campuslab.kafkaadmin.dto.TopicRequest;
import org.campuslab.kafkaadmin.dto.TopicResponse;
import org.campuslab.kafkaadmin.dto.TopicSummary;
import org.campuslab.kafkaadmin.service.MessageInspector;
import org.campuslab.kafkaadmin.service.TopicAdminService;
import org.campuslab.kafkaadmin.service.TopicProvisioningService;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

/**
 * Recurso "tópicos". Solo delega en los servicios: no conoce Kafka.
 *
 * <pre>
 *  GET    /api/kafka/topics                    lista los tópicos
 *  POST   /api/kafka/topics                    crea un tópico
 *  POST   /api/kafka/topics/provision          crea/alinea la topología del proyecto (idempotente)
 *  GET    /api/kafka/topics/{name}             particiones (líder, réplicas, ISR, offsets) y configuración
 *  PATCH  /api/kafka/topics/{name}/config      modifica retención, cleanup.policy, min.insync.replicas...
 *  PUT    /api/kafka/topics/{name}/partitions  aumenta las particiones
 *  GET    /api/kafka/topics/{name}/messages    últimos mensajes (para revisar una DLT)
 *  DELETE /api/kafka/topics/{name}             elimina el tópico
 * </pre>
 */
@RestController
@RequestMapping("/api/kafka/topics")
@PreAuthorize("hasRole('ADMIN')")
@Tag(name = "Tópicos", description = "Crear, consultar, modificar y eliminar tópicos de Kafka y sus configuraciones")
public class TopicController {

    private static final String INVALID_NAME = "Nombre de tópico inválido";

    private final TopicAdminService topics;
    private final TopicProvisioningService provisioning;
    private final MessageInspector inspector;

    public TopicController(TopicAdminService topics, TopicProvisioningService provisioning, MessageInspector inspector) {
        this.topics = topics;
        this.provisioning = provisioning;
        this.inspector = inspector;
    }

    @GetMapping
    @Operation(summary = "Listar tópicos", description = "Con includeInternal=true también muestra los tópicos internos (__consumer_offsets...).")
    @ApiResponse(responseCode = "200", description = "Nombre, particiones y factor de replicación de cada tópico")
    @ApiResponse(responseCode = "503", description = "El clúster no responde")
    public List<TopicSummary> list(@RequestParam(defaultValue = "false") boolean includeInternal) {
        return topics.list(includeInternal);
    }

    @PostMapping
    @Operation(summary = "Crear un tópico",
            description = "Solo el nombre es obligatorio (por defecto 3 particiones y factor de replicación 3). "
                    + "Configuraciones permitidas: cleanup.policy, retention.ms, retention.bytes, min.insync.replicas, segment.bytes, "
                    + "segment.ms, max.message.bytes, delete.retention.ms, min.cleanable.dirty.ratio, compression.type, "
                    + "message.timestamp.type, unclean.leader.election.enable.")
    @ApiResponse(responseCode = "201", description = "Tópico creado")
    @ApiResponse(responseCode = "400", description = "Nombre, particiones o configuración inválidos")
    @ApiResponse(responseCode = "409", description = "El tópico ya existe")
    @ApiResponse(responseCode = "422", description = "Más réplicas que brokers, o min.insync.replicas mayor que las réplicas")
    public ResponseEntity<TopicResponse> create(@Valid @RequestBody TopicRequest request) {
        TopicResponse created = topics.create(request);
        URI location = ServletUriComponentsBuilder.fromCurrentRequest().path("/{name}").build(created.name());
        return ResponseEntity.created(location).body(created);
    }

    @PostMapping("/provision")
    @Operation(summary = "Aprovisionar la topología del proyecto",
            description = "Crea los tópicos que faltan (bookings.events, audit.timeline y sus DLT) y alinea la configuración de los que ya "
                    + "existen. Idempotente: se puede ejecutar las veces que se quiera y en cualquier clúster.")
    @ApiResponse(responseCode = "200", description = "Qué se creó, qué se actualizó y qué ya estaba correcto")
    public ProvisionResponse provision() {
        return provisioning.provision();
    }

    @GetMapping("/{name}")
    @Operation(summary = "Describir un tópico", description = "Particiones con líder, réplicas, ISR y offsets, más la configuración efectiva.")
    @ApiResponse(responseCode = "200", description = "Detalle del tópico")
    @ApiResponse(responseCode = "404", description = "El tópico no existe")
    public TopicDetailResponse describe(@PathVariable @Pattern(regexp = NamePatterns.TOPIC_NAME, message = INVALID_NAME) String name) {
        return topics.describe(name);
    }

    @PatchMapping("/{name}/config")
    @Operation(summary = "Modificar la configuración de un tópico",
            description = "'set' fija valores y 'reset' devuelve una clave a su valor por defecto. Devuelve el tópico actualizado.")
    @ApiResponse(responseCode = "200", description = "Tópico con la configuración ya aplicada")
    @ApiResponse(responseCode = "400", description = "Clave no administrable o valor fuera de rango")
    @ApiResponse(responseCode = "404", description = "El tópico no existe")
    @ApiResponse(responseCode = "422", description = "min.insync.replicas mayor que el factor de replicación")
    public TopicDetailResponse updateConfig(
            @PathVariable @Pattern(regexp = NamePatterns.TOPIC_NAME, message = INVALID_NAME) String name,
            @RequestBody TopicConfigUpdateRequest request) {
        return topics.updateConfig(name, request);
    }

    @PutMapping("/{name}/partitions")
    @Operation(summary = "Aumentar las particiones de un tópico",
            description = "Kafka solo permite aumentarlas. Ojo: cambia a qué partición va cada clave (hash(clave) % particiones).")
    @ApiResponse(responseCode = "200", description = "Tópico con las nuevas particiones")
    @ApiResponse(responseCode = "404", description = "El tópico no existe")
    @ApiResponse(responseCode = "422", description = "Se pidió igual o menos particiones de las que ya tiene")
    public TopicDetailResponse increasePartitions(
            @PathVariable @Pattern(regexp = NamePatterns.TOPIC_NAME, message = INVALID_NAME) String name,
            @Valid @RequestBody PartitionsRequest request) {
        return topics.increasePartitions(name, request.partitions());
    }

    @GetMapping("/{name}/messages")
    @Operation(summary = "Ver los últimos mensajes de un tópico",
            description = "Solo lectura: no crea un consumer group ni mueve offsets. En una DLT los headers explican la causa del fallo.")
    @ApiResponse(responseCode = "200", description = "Mensajes, del más reciente al más antiguo")
    @ApiResponse(responseCode = "404", description = "El tópico no existe")
    public List<MessageView> messages(
            @PathVariable @Pattern(regexp = NamePatterns.TOPIC_NAME, message = INVALID_NAME) String name,
            @RequestParam(defaultValue = "20") @Min(value = 1, message = "limit mínimo 1")
            @Max(value = MessageInspector.MAX_LIMIT, message = "limit máximo 100") int limit) {
        return inspector.latest(name, limit);
    }

    @DeleteMapping("/{name}")
    @Operation(summary = "Eliminar un tópico", description = "Elimina el tópico y todos sus mensajes. Los tópicos internos (__*) están protegidos.")
    @ApiResponse(responseCode = "204", description = "Tópico eliminado")
    @ApiResponse(responseCode = "404", description = "El tópico no existe")
    @ApiResponse(responseCode = "422", description = "Es un tópico interno de Kafka")
    public ResponseEntity<Void> delete(@PathVariable @Pattern(regexp = NamePatterns.TOPIC_NAME, message = INVALID_NAME) String name) {
        topics.delete(name);
        return ResponseEntity.noContent().build();
    }
}
