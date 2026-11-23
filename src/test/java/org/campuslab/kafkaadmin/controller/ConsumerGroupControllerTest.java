package org.campuslab.kafkaadmin.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Instant;
import java.util.List;

import org.campuslab.kafkaadmin.dto.BrokerInfo;
import org.campuslab.kafkaadmin.dto.ClusterResponse;
import org.campuslab.kafkaadmin.dto.ConsumerGroupDetail;
import org.campuslab.kafkaadmin.dto.ConsumerGroupSummary;
import org.campuslab.kafkaadmin.dto.GroupMemberInfo;
import org.campuslab.kafkaadmin.dto.OffsetChange;
import org.campuslab.kafkaadmin.dto.OffsetResetStrategy;
import org.campuslab.kafkaadmin.dto.PartitionLag;
import org.campuslab.kafkaadmin.dto.ResetOffsetsRequest;
import org.campuslab.kafkaadmin.dto.ResetOffsetsResponse;
import org.campuslab.kafkaadmin.dto.TopicPartitionRef;
import org.campuslab.kafkaadmin.exception.ResourceConflictException;
import org.campuslab.kafkaadmin.exception.ResourceNotFoundException;
import org.campuslab.kafkaadmin.security.SecurityConfig;
import org.campuslab.kafkaadmin.service.ClusterAdminService;
import org.campuslab.kafkaadmin.service.ConsumerGroupAdminService;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.RequestPostProcessor;

/** Capa web de los recursos consumer groups y clúster (los servicios van mockeados). */
@WebMvcTest({ConsumerGroupController.class, ClusterController.class})
@Import({SecurityConfig.class, FakeJwtDecoderConfig.class})
class ConsumerGroupControllerTest {

    @Autowired
    private MockMvc mockMvc;
    @MockBean
    private ConsumerGroupAdminService groups;
    @MockBean
    private ClusterAdminService cluster;

    private static RequestPostProcessor admin() {
        return jwt().authorities(new SimpleGrantedAuthority("ROLE_ADMIN"));
    }

    // ---------- seguridad ----------

    @Test
    void sinTokenEs401YSinRolAdminEs403() throws Exception {
        RequestPostProcessor auditor = jwt().authorities(new SimpleGrantedAuthority("ROLE_AUDITOR"));
        mockMvc.perform(get("/api/kafka/consumer-groups")).andExpect(status().isUnauthorized());
        mockMvc.perform(get("/api/kafka/consumer-groups").with(auditor)).andExpect(status().isForbidden());
        mockMvc.perform(get("/api/kafka/cluster").with(auditor)).andExpect(status().isForbidden());
        mockMvc.perform(delete("/api/kafka/consumer-groups/audit-group").with(auditor)).andExpect(status().isForbidden());
        verifyNoInteractions(groups, cluster);
    }

    // ---------- consulta ----------

    @Test
    void listaLosGruposConSuEstado() throws Exception {
        when(groups.list()).thenReturn(List.of(new ConsumerGroupSummary("audit-group", "STABLE"),
                new ConsumerGroupSummary("report-group", "EMPTY")));

        mockMvc.perform(get("/api/kafka/consumer-groups").with(admin()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].groupId").value("audit-group"))
                .andExpect(jsonPath("$[1].state").value("EMPTY"));
    }

    @Test
    void describeUnGrupoConMiembrosYLag() throws Exception {
        when(groups.describe("audit-group")).thenReturn(new ConsumerGroupDetail("audit-group", "STABLE", "cooperative-sticky", 2,
                List.of(new GroupMemberInfo("m-1", "audit-0", "/10.0.0.5", null, List.of(new TopicPartitionRef("bookings.events", 0)))),
                List.of(new PartitionLag("bookings.events", 0, 8, 10, 2)), 2));

        mockMvc.perform(get("/api/kafka/consumer-groups/audit-group").with(admin()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("STABLE"))
                .andExpect(jsonPath("$.members[0].assignedPartitions[0].topic").value("bookings.events"))
                .andExpect(jsonPath("$.offsets[0].lag").value(2))
                .andExpect(jsonPath("$.totalLag").value(2));
    }

    @Test
    void unGrupoInexistenteEs404YUnIdentificadorInvalidoEs400() throws Exception {
        when(groups.describe("nada")).thenThrow(new ResourceNotFoundException("No existe el consumer group 'nada'"));

        mockMvc.perform(get("/api/kafka/consumer-groups/nada").with(admin()))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.message").value("No existe el consumer group 'nada'"));
        mockMvc.perform(get("/api/kafka/consumer-groups/mal grupo").with(admin()))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.groupId").value("Identificador de grupo inválido"));
    }

    @Test
    void describeElCluster() throws Exception {
        when(cluster.describe()).thenReturn(new ClusterResponse("abc123", 1,
                List.of(new BrokerInfo(1, "kafka1", 29092, null), new BrokerInfo(2, "kafka2", 29092, null))));

        mockMvc.perform(get("/api/kafka/cluster").with(admin()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.clusterId").value("abc123"))
                .andExpect(jsonPath("$.controllerId").value(1))
                .andExpect(jsonPath("$.brokers.length()").value(2));
    }

    // ---------- reinicio de offsets ----------

    @Test
    void reiniciaLosOffsets() throws Exception {
        when(groups.resetOffsets(eq("audit-group"), any())).thenReturn(new ResetOffsetsResponse("audit-group", "bookings.events",
                OffsetResetStrategy.EARLIEST, false, true, List.of(new OffsetChange(0, 8L, 0))));

        mockMvc.perform(post("/api/kafka/consumer-groups/audit-group/offsets").with(admin()).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"bookings.events\",\"strategy\":\"EARLIEST\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.applied").value(true))
                .andExpect(jsonPath("$.changes[0].from").value(8))
                .andExpect(jsonPath("$.changes[0].to").value(0));

        ArgumentCaptor<ResetOffsetsRequest> sent = ArgumentCaptor.forClass(ResetOffsetsRequest.class);
        verify(groups).resetOffsets(eq("audit-group"), sent.capture());
        assertThat(sent.getValue().dryRun()).isFalse();           // por defecto SÍ aplica...
        assertThat(sent.getValue().partitions()).isEmpty();       // ...y mueve todas las particiones
    }

    @Test
    void elTimestampSeInterpretaComoIso8601() throws Exception {
        mockMvc.perform(post("/api/kafka/consumer-groups/g/offsets").with(admin()).contentType(MediaType.APPLICATION_JSON)
                .content("{\"topic\":\"t\",\"strategy\":\"TIMESTAMP\",\"timestamp\":\"2026-11-20T00:00:00Z\",\"dryRun\":true}"));

        ArgumentCaptor<ResetOffsetsRequest> sent = ArgumentCaptor.forClass(ResetOffsetsRequest.class);
        verify(groups).resetOffsets(eq("g"), sent.capture());
        assertThat(sent.getValue().timestamp()).isEqualTo(Instant.parse("2026-11-20T00:00:00Z"));
        assertThat(sent.getValue().dryRun()).isTrue();
    }

    @Test
    void elReinicioConDatosInvalidosEs400() throws Exception {
        mockMvc.perform(post("/api/kafka/consumer-groups/g/offsets").with(admin()).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"strategy\":\"EARLIEST\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.topic").exists());
        mockMvc.perform(post("/api/kafka/consumer-groups/g/offsets").with(admin()).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"t\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.strategy").exists());
        mockMvc.perform(post("/api/kafka/consumer-groups/g/offsets").with(admin()).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"t\",\"strategy\":\"A_LA_LUNA\"}"))
                .andExpect(status().isBadRequest());
        mockMvc.perform(post("/api/kafka/consumer-groups/g/offsets").with(admin()).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"t\",\"strategy\":\"OFFSET\",\"offset\":-3}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.offset").exists());
        verifyNoInteractions(groups);
    }

    @Test
    void reiniciarUnGrupoConConsumidoresActivosEs409() throws Exception {
        when(groups.resetOffsets(eq("audit-group"), any()))
                .thenThrow(new ResourceConflictException("El consumer group 'audit-group' está STABLE"));

        mockMvc.perform(post("/api/kafka/consumer-groups/audit-group/offsets").with(admin()).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"bookings.events\",\"strategy\":\"LATEST\"}"))
                .andExpect(status().isConflict());
    }

    // ---------- eliminación ----------

    @Test
    void eliminaUnGrupo() throws Exception {
        mockMvc.perform(delete("/api/kafka/consumer-groups/old-group").with(admin())).andExpect(status().isNoContent());
        verify(groups).delete("old-group");
    }

    @Test
    void eliminarUnGrupoActivoEs409() throws Exception {
        org.mockito.Mockito.doThrow(new ResourceConflictException("tiene consumidores activos")).when(groups).delete("audit-group");

        mockMvc.perform(delete("/api/kafka/consumer-groups/audit-group").with(admin())).andExpect(status().isConflict());
    }
}
