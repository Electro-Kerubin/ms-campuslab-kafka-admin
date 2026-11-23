package org.campuslab.kafkaadmin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.campuslab.kafkaadmin.config.ManagedTopicsProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.test.web.servlet.MockMvc;

/**
 * Arranca la aplicación completa (seguridad, validación, springdoc, propiedades y el AdminClient).
 * No necesita broker: el AdminClient se conecta de forma perezosa y nadie lo usa en estos tests.
 */
@SpringBootTest
@AutoConfigureMockMvc
class KafkaAdminApplicationTests {

    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private ManagedTopicsProperties managed;

    @Test
    void laDocumentacionOpenApiDescribeTodosLosEndpointsDeAdministracion() throws Exception {
        mockMvc.perform(get("/v3/api-docs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.info.title").value("CampusLab - Administrador de Kafka"))
                .andExpect(jsonPath("$.paths['/api/kafka/topics'].get").exists())
                .andExpect(jsonPath("$.paths['/api/kafka/topics'].post").exists())
                .andExpect(jsonPath("$.paths['/api/kafka/topics/provision'].post").exists())
                .andExpect(jsonPath("$.paths['/api/kafka/topics/{name}'].get").exists())
                .andExpect(jsonPath("$.paths['/api/kafka/topics/{name}'].delete").exists())
                .andExpect(jsonPath("$.paths['/api/kafka/topics/{name}/config'].patch").exists())
                .andExpect(jsonPath("$.paths['/api/kafka/topics/{name}/partitions'].put").exists())
                .andExpect(jsonPath("$.paths['/api/kafka/topics/{name}/messages'].get").exists())
                .andExpect(jsonPath("$.paths['/api/kafka/consumer-groups'].get").exists())
                .andExpect(jsonPath("$.paths['/api/kafka/consumer-groups/{groupId}'].get").exists())
                .andExpect(jsonPath("$.paths['/api/kafka/consumer-groups/{groupId}'].delete").exists())
                .andExpect(jsonPath("$.paths['/api/kafka/consumer-groups/{groupId}/offsets'].post").exists())
                .andExpect(jsonPath("$.paths['/api/kafka/cluster'].get").exists());
    }

    @Test
    void laApiDeAdministracionNoEsPublica() throws Exception {
        mockMvc.perform(get("/api/kafka/topics")).andExpect(status().isUnauthorized());
    }

    @Test
    void cualquierOtraRutaEstaBloqueada() throws Exception {
        mockMvc.perform(get("/api/otra-cosa").with(jwt().authorities(new SimpleGrantedAuthority("ROLE_ADMIN"))))
                .andExpect(status().isForbidden());
    }

    @Test
    void laTopologiaDelProyectoVieneDeApplicationYml() {
        assertThat(managed.topics()).extracting(ManagedTopicsProperties.ManagedTopic::name)
                .containsExactly("bookings.events", "audit.timeline", "bookings.events.DLT", "audit.timeline.DLT");

        var events = managed.topics().get(0);
        assertThat(events.partitions()).isEqualTo(3);
        assertThat(events.replicationFactor()).isEqualTo((short) 3);
        assertThat(events.configs()).containsEntry("cleanup.policy", "delete").containsEntry("retention.ms", "604800000");

        var timeline = managed.topics().get(1);
        assertThat(timeline.configs()).containsEntry("cleanup.policy", "compact,delete");
    }
}
