package org.campuslab.kafkaadmin.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.campuslab.kafkaadmin.dto.ClusterResponse;
import org.campuslab.kafkaadmin.service.ClusterAdminService;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** GET /api/kafka/cluster: brokers vivos y controlador. */
@RestController
@RequestMapping("/api/kafka/cluster")
@PreAuthorize("hasRole('ADMIN')")
@Tag(name = "Clúster", description = "Estado de los brokers")
public class ClusterController {

    private final ClusterAdminService cluster;

    public ClusterController(ClusterAdminService cluster) {
        this.cluster = cluster;
    }

    @GetMapping
    @Operation(summary = "Estado del clúster", description = "Identificador, controlador y brokers que responden.")
    @ApiResponse(responseCode = "200", description = "Brokers del clúster")
    @ApiResponse(responseCode = "503", description = "El clúster no responde")
    public ClusterResponse describe() {
        return cluster.describe();
    }
}
