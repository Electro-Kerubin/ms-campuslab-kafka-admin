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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.campuslab.kafkaadmin.dto.ConfigValue;
import org.campuslab.kafkaadmin.dto.MessageView;
import org.campuslab.kafkaadmin.dto.PartitionInfo;
import org.campuslab.kafkaadmin.dto.ProvisionResponse;
import org.campuslab.kafkaadmin.dto.TopicConfigUpdateRequest;
import org.campuslab.kafkaadmin.dto.TopicDetailResponse;
import org.campuslab.kafkaadmin.dto.TopicProvisionResult;
import org.campuslab.kafkaadmin.dto.TopicRequest;
import org.campuslab.kafkaadmin.dto.TopicResponse;
import org.campuslab.kafkaadmin.dto.TopicSummary;
import org.campuslab.kafkaadmin.exception.BrokerUnavailableException;
import org.campuslab.kafkaadmin.exception.InvalidConfigurationException;
import org.campuslab.kafkaadmin.exception.ResourceConflictException;
import org.campuslab.kafkaadmin.exception.ResourceNotFoundException;
import org.campuslab.kafkaadmin.exception.UnprocessableOperationException;
import org.campuslab.kafkaadmin.security.SecurityConfig;
import org.campuslab.kafkaadmin.service.MessageInspector;
import org.campuslab.kafkaadmin.service.TopicAdminService;
import org.campuslab.kafkaadmin.service.TopicProvisioningService;
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

/**
 * Capa web del recurso tópicos: rutas, verbos, códigos HTTP, seguridad y formato de los errores.
 * Los servicios van mockeados: no hace falta Kafka.
 */
@WebMvcTest(TopicController.class)
@Import({SecurityConfig.class, FakeJwtDecoderConfig.class})
class TopicControllerTest {

    @Autowired
    private MockMvc mockMvc;
    @MockBean
    private TopicAdminService topics;
    @MockBean
    private TopicProvisioningService provisioning;
    @MockBean
    private MessageInspector inspector;

    private static RequestPostProcessor admin() {
        return jwt().authorities(new SimpleGrantedAuthority("ROLE_ADMIN"));
    }

    private static TopicDetailResponse detail() {
        return new TopicDetailResponse("bookings.events", false, 3, 10,
                List.of(new PartitionInfo(0, 1, List.of(1, 2, 3), List.of(1, 2, 3), 0, 10, 10)),
                Map.of("retention.ms", new ConfigValue("604800000", "DYNAMIC_TOPIC_CONFIG", false)));
    }

    // ---------- seguridad ----------

    @Test
    void sinTokenSeResponde401() throws Exception {
        mockMvc.perform(get("/api/kafka/topics")).andExpect(status().isUnauthorized());
        verifyNoInteractions(topics);
    }

    @Test
    void conTokenSinRolAdminSeResponde403EnTodasLasOperaciones() throws Exception {
        RequestPostProcessor student = jwt().authorities(new SimpleGrantedAuthority("ROLE_ESTUDIANTE"));
        mockMvc.perform(get("/api/kafka/topics").with(student)).andExpect(status().isForbidden())
                .andExpect(jsonPath("$.message").value("No tienes permisos para administrar Kafka"));
        mockMvc.perform(post("/api/kafka/topics").with(student).contentType(MediaType.APPLICATION_JSON).content("{\"name\":\"x\"}"))
                .andExpect(status().isForbidden());
        mockMvc.perform(delete("/api/kafka/topics/x").with(student)).andExpect(status().isForbidden());
        mockMvc.perform(post("/api/kafka/topics/provision").with(student)).andExpect(status().isForbidden());
        verifyNoInteractions(topics, provisioning, inspector);
    }

    // ---------- GET ----------

    @Test
    void listaLosTopicos() throws Exception {
        when(topics.list(false)).thenReturn(List.of(new TopicSummary("audit.timeline", 3, 3, false)));

        mockMvc.perform(get("/api/kafka/topics").with(admin()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].name").value("audit.timeline"))
                .andExpect(jsonPath("$[0].partitions").value(3))
                .andExpect(jsonPath("$[0].replicationFactor").value(3));
    }

    @Test
    void includeInternalSePasaAlServicio() throws Exception {
        mockMvc.perform(get("/api/kafka/topics").with(admin()).param("includeInternal", "true")).andExpect(status().isOk());
        verify(topics).list(true);
    }

    @Test
    void describeUnTopico() throws Exception {
        when(topics.describe("bookings.events")).thenReturn(detail());

        mockMvc.perform(get("/api/kafka/topics/bookings.events").with(admin()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.partitions[0].leader").value(1))
                .andExpect(jsonPath("$.partitions[0].inSyncReplicas.length()").value(3))
                .andExpect(jsonPath("$.configs['retention.ms'].value").value("604800000"))
                .andExpect(jsonPath("$.totalMessages").value(10));
    }

    @Test
    void unTopicoInexistenteEs404ConElFormatoDeErrorDelProyecto() throws Exception {
        when(topics.describe("nada")).thenThrow(new ResourceNotFoundException("No existe el tópico 'nada'"));

        mockMvc.perform(get("/api/kafka/topics/nada").with(admin()))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404))
                .andExpect(jsonPath("$.message").value("No existe el tópico 'nada'"))
                .andExpect(jsonPath("$.timestamp").exists());
    }

    @Test
    void unNombreConCaracteresInvalidosEnLaRutaSeRechazaSinLlegarAlCluster() throws Exception {
        mockMvc.perform(get("/api/kafka/topics/mal$nombre").with(admin()))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.name").value("Nombre de tópico inválido"));
        verifyNoInteractions(topics);
    }

    @Test
    void conElClusterCaidoSeResponde503() throws Exception {
        when(topics.list(false)).thenThrow(new BrokerUnavailableException("No se pudo contactar al clúster", new RuntimeException()));

        mockMvc.perform(get("/api/kafka/topics").with(admin()))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.message").value("No se pudo contactar al clúster"));
    }

    // ---------- POST ----------

    @Test
    void creaUnTopicoYResponde201ConLocation() throws Exception {
        when(topics.create(any())).thenReturn(new TopicResponse("bookings.events", 3, 3, Map.of("retention.ms", "604800000")));

        mockMvc.perform(post("/api/kafka/topics").with(admin()).contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"name":"bookings.events","partitions":3,"replicationFactor":3,
                                 "configs":{"retention.ms":"604800000"}}"""))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", "http://localhost/api/kafka/topics/bookings.events"))
                .andExpect(jsonPath("$.name").value("bookings.events"))
                .andExpect(jsonPath("$.configs['retention.ms']").value("604800000"));
    }

    @Test
    void soloElNombreEsObligatorioYElRestoTomaLosValoresDelProyecto() throws Exception {
        when(topics.create(any())).thenReturn(new TopicResponse("simple", 3, 3, Map.of()));

        mockMvc.perform(post("/api/kafka/topics").with(admin()).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"simple\"}"))
                .andExpect(status().isCreated());

        ArgumentCaptor<TopicRequest> sent = ArgumentCaptor.forClass(TopicRequest.class);
        verify(topics).create(sent.capture());
        assertThat(sent.getValue().partitions()).isEqualTo(3);
        assertThat(sent.getValue().replicationFactor()).isEqualTo((short) 3);
        assertThat(sent.getValue().configs()).isEmpty();
    }

    @Test
    void losDatosInvalidosSeRechazanListandoCadaCampoYNoLlegaAlCluster() throws Exception {
        mockMvc.perform(post("/api/kafka/topics").with(admin()).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"con espacio\",\"partitions\":0,\"replicationFactor\":11}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("La petición tiene datos inválidos"))
                .andExpect(jsonPath("$.errors.name").exists())
                .andExpect(jsonPath("$.errors.partitions").exists())
                .andExpect(jsonPath("$.errors.replicationFactor").exists());
        mockMvc.perform(post("/api/kafka/topics").with(admin()).contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.name").value(org.hamcrest.Matchers.containsString("obligatorio")));
        verifyNoInteractions(topics);
    }

    @Test
    void unTopicoQueYaExisteEs409() throws Exception {
        when(topics.create(any())).thenThrow(new ResourceConflictException("El tópico 'x' ya existe"));

        mockMvc.perform(post("/api/kafka/topics").with(admin()).contentType(MediaType.APPLICATION_JSON).content("{\"name\":\"x\"}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.message").value("El tópico 'x' ya existe"));
    }

    @Test
    void masReplicasQueBrokersEs422() throws Exception {
        when(topics.create(any())).thenThrow(new UnprocessableOperationException("El factor de replicación (3) no puede superar los brokers (1)"));

        mockMvc.perform(post("/api/kafka/topics").with(admin()).contentType(MediaType.APPLICATION_JSON).content("{\"name\":\"x\"}"))
                .andExpect(status().isUnprocessableEntity());
    }

    @Test
    void unaConfiguracionNoPermitidaEs400() throws Exception {
        when(topics.create(any())).thenThrow(new InvalidConfigurationException("La configuración 'x' no se puede administrar"));

        mockMvc.perform(post("/api/kafka/topics").with(admin()).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"x\",\"configs\":{\"x\":\"1\"}}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void unCuerpoQueNoEsJsonEs400YOtroTipoDeContenidoEs415() throws Exception {
        mockMvc.perform(post("/api/kafka/topics").with(admin()).contentType(MediaType.APPLICATION_JSON).content("{no es json"))
                .andExpect(status().isBadRequest());
        mockMvc.perform(post("/api/kafka/topics").with(admin()).contentType(MediaType.TEXT_PLAIN).content("x"))
                .andExpect(status().isUnsupportedMediaType());
    }

    @Test
    void aprovisionaLaTopologia() throws Exception {
        when(provisioning.provision()).thenReturn(new ProvisionResponse(3, 1, 1, 2,
                List.of(new TopicProvisionResult("bookings.events", "CREATED", List.of("Creado"), List.of()))));

        mockMvc.perform(post("/api/kafka/topics/provision").with(admin()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.brokers").value(3))
                .andExpect(jsonPath("$.created").value(1))
                .andExpect(jsonPath("$.unchanged").value(2))
                .andExpect(jsonPath("$.topics[0].action").value("CREATED"));
    }

    // ---------- PATCH / PUT ----------

    @Test
    void modificaLaConfiguracion() throws Exception {
        when(topics.updateConfig(eq("bookings.events"), any())).thenReturn(detail());

        mockMvc.perform(patch("/api/kafka/topics/bookings.events/config").with(admin()).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"set\":{\"retention.ms\":\"86400000\"},\"reset\":[\"retention.bytes\"]}"))
                .andExpect(status().isOk());

        ArgumentCaptor<TopicConfigUpdateRequest> sent = ArgumentCaptor.forClass(TopicConfigUpdateRequest.class);
        verify(topics).updateConfig(eq("bookings.events"), sent.capture());
        assertThat(sent.getValue().set()).containsEntry("retention.ms", "86400000");
        assertThat(sent.getValue().reset()).containsExactly("retention.bytes");
    }

    @Test
    void aumentaLasParticiones() throws Exception {
        when(topics.increasePartitions("bookings.events", 6)).thenReturn(detail());

        mockMvc.perform(put("/api/kafka/topics/bookings.events/partitions").with(admin()).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"partitions\":6}"))
                .andExpect(status().isOk());
        verify(topics).increasePartitions("bookings.events", 6);
    }

    @Test
    void lasParticionesInvalidasSeRechazanYReducirEs422() throws Exception {
        mockMvc.perform(put("/api/kafka/topics/x/partitions").with(admin()).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"partitions\":0}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.partitions").exists());
        mockMvc.perform(put("/api/kafka/topics/x/partitions").with(admin()).contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(topics);

        when(topics.increasePartitions("x", 1)).thenThrow(new UnprocessableOperationException("Kafka no permite reducir particiones"));
        mockMvc.perform(put("/api/kafka/topics/x/partitions").with(admin()).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"partitions\":1}"))
                .andExpect(status().isUnprocessableEntity());
    }

    // ---------- mensajes ----------

    @Test
    void veLosUltimosMensajes() throws Exception {
        when(inspector.latest("bookings.events.DLT", 5)).thenReturn(List.of(
                new MessageView(0, 7, Instant.parse("2026-11-20T10:00:00Z"), "42", "{\"x\":1}",
                        Map.of("kafka_dlt-exception-message", "boom"))));

        mockMvc.perform(get("/api/kafka/topics/bookings.events.DLT/messages").with(admin()).param("limit", "5"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].offset").value(7))
                .andExpect(jsonPath("$[0].key").value("42"))
                .andExpect(jsonPath("$[0].headers['kafka_dlt-exception-message']").value("boom"));
    }

    @Test
    void elLimiteDeMensajesTieneRango() throws Exception {
        mockMvc.perform(get("/api/kafka/topics/x/messages").with(admin()).param("limit", "0")).andExpect(status().isBadRequest());
        mockMvc.perform(get("/api/kafka/topics/x/messages").with(admin()).param("limit", "101")).andExpect(status().isBadRequest());
        verifyNoInteractions(inspector);
    }

    // ---------- DELETE ----------

    @Test
    void eliminaUnTopico() throws Exception {
        mockMvc.perform(delete("/api/kafka/topics/bookings.events").with(admin())).andExpect(status().isNoContent());
        verify(topics).delete("bookings.events");
    }

    @Test
    void eliminarUnTopicoInternoEs422YUnoInexistenteEs404() throws Exception {
        org.mockito.Mockito.doThrow(new UnprocessableOperationException("es un tópico interno")).when(topics).delete("__consumer_offsets");
        org.mockito.Mockito.doThrow(new ResourceNotFoundException("No existe")).when(topics).delete("nada");

        mockMvc.perform(delete("/api/kafka/topics/__consumer_offsets").with(admin())).andExpect(status().isUnprocessableEntity());
        mockMvc.perform(delete("/api/kafka/topics/nada").with(admin())).andExpect(status().isNotFound());
    }

    @Test
    void unVerboNoPermitidoEs405() throws Exception {
        mockMvc.perform(put("/api/kafka/topics").with(admin()).contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isMethodNotAllowed());
    }
}
