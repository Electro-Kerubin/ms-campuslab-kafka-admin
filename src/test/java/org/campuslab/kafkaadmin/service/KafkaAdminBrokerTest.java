package org.campuslab.kafkaadmin.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.campuslab.kafkaadmin.dto.ConsumerGroupDetail;
import org.campuslab.kafkaadmin.dto.MessageView;
import org.campuslab.kafkaadmin.dto.OffsetResetStrategy;
import org.campuslab.kafkaadmin.dto.ProvisionResponse;
import org.campuslab.kafkaadmin.dto.ResetOffsetsRequest;
import org.campuslab.kafkaadmin.dto.ResetOffsetsResponse;
import org.campuslab.kafkaadmin.dto.TopicConfigUpdateRequest;
import org.campuslab.kafkaadmin.dto.TopicDetailResponse;
import org.campuslab.kafkaadmin.dto.TopicRequest;
import org.campuslab.kafkaadmin.exception.InvalidConfigurationException;
import org.campuslab.kafkaadmin.exception.ResourceConflictException;
import org.campuslab.kafkaadmin.exception.ResourceNotFoundException;
import org.campuslab.kafkaadmin.exception.UnprocessableOperationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

/**
 * Los servicios contra un broker Kafka REAL (embebido en la JVM del test, sin Docker): crear/describir/modificar/
 * eliminar tópicos, aprovisionar la topología, y consumer groups con lag y reinicio de offsets.
 *
 * El broker embebido tiene 1 solo nodo: por eso los tópicos usan factor de replicación 1 y el aprovisionamiento
 * demuestra el ajuste automático desde el factor 3 del proyecto.
 */
@SpringBootTest(properties = "campuslab.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(partitions = 1, count = 1)
@DirtiesContext
class KafkaAdminBrokerTest {

    @Autowired
    private TopicAdminService topics;
    @Autowired
    private TopicProvisioningService provisioning;
    @Autowired
    private ConsumerGroupAdminService groups;
    @Autowired
    private ClusterAdminService cluster;
    @Autowired
    private MessageInspector inspector;
    @Autowired
    private EmbeddedKafkaBroker broker;

    private static String unique(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    // ---------- Clúster ----------

    @Test
    void elClusterInformaSusBrokers() {
        var description = cluster.describe();

        assertThat(description.clusterId()).isNotBlank();
        assertThat(description.brokers()).hasSize(1);
        assertThat(description.brokers().get(0).host()).isNotBlank();
    }

    // ---------- Tópicos ----------

    @Test
    void creaUnTopicoConSusParticionesYConfiguracionYLoDescribe() {
        String name = unique("t-crear");

        var created = topics.create(new TopicRequest(name, 3, (short) 1,
                Map.of("cleanup.policy", "compact,delete", "retention.ms", "86400000", "min.insync.replicas", "1")));

        assertThat(created.name()).isEqualTo(name);
        assertThat(created.partitions()).isEqualTo(3);
        awaitTopic(name);
        TopicDetailResponse detail = topics.describe(name);
        assertThat(detail.partitions()).hasSize(3);
        assertThat(detail.replicationFactor()).isEqualTo(1);
        assertThat(detail.partitions().get(0).leader()).isNotNull();
        assertThat(detail.partitions().get(0).inSyncReplicas()).hasSize(1);
        assertThat(detail.configs().get("retention.ms").value()).isEqualTo("86400000");
        assertThat(detail.configs().get("retention.ms").source()).isEqualTo("DYNAMIC_TOPIC_CONFIG");
        assertThat(detail.configs().get("cleanup.policy").value()).isEqualTo("compact,delete");
        assertThat(topics.list(false)).extracting("name").contains(name);
    }

    @Test
    void siNoSeIndicaNadaMasQueElNombreSeUsaLaTopologiaPorDefectoDelProyecto() {
        var request = new TopicRequest(unique("t-def"), null, null, null);

        assertThat(request.partitions()).isEqualTo(3);
        assertThat(request.replicationFactor()).isEqualTo((short) 3);
        // ...y como el broker de prueba tiene 1 nodo, el clúster lo rechaza con un mensaje claro
        assertThatThrownBy(() -> topics.create(request))
                .isInstanceOf(UnprocessableOperationException.class)
                .hasMessageContaining("factor de replicación (3)")
                .hasMessageContaining("brokers del clúster (1)");
    }

    @Test
    void crearUnTopicoQueYaExisteEsUnConflicto() {
        String name = unique("t-dup");
        topics.create(new TopicRequest(name, 1, (short) 1, Map.of()));

        assertThatThrownBy(() -> topics.create(new TopicRequest(name, 1, (short) 1, Map.of())))
                .isInstanceOf(ResourceConflictException.class)
                .hasMessageContaining(name);
    }

    @Test
    void unaConfiguracionInvalidaSeRechazaAntesDeLlegarAlClusterYNoCreaNada() {
        String name = unique("t-bad");

        assertThatThrownBy(() -> topics.create(new TopicRequest(name, 1, (short) 1, Map.of("retention.ms", "mucho"))))
                .isInstanceOf(InvalidConfigurationException.class).hasMessageContaining("retention.ms");
        assertThatThrownBy(() -> topics.create(new TopicRequest(name, 1, (short) 1, Map.of("inventada", "1"))))
                .isInstanceOf(InvalidConfigurationException.class).hasMessageContaining("no se puede administrar");
        assertThatThrownBy(() -> topics.create(new TopicRequest(name, 1, (short) 1, Map.of("min.insync.replicas", "2"))))
                .isInstanceOf(UnprocessableOperationException.class).hasMessageContaining("min.insync.replicas");

        assertThat(topics.list(false)).extracting("name").doesNotContain(name);
    }

    @Test
    void modificaYRestableceLaConfiguracionDeUnTopico() {
        String name = unique("t-cfg");
        topics.create(new TopicRequest(name, 1, (short) 1, Map.of("retention.ms", "86400000")));
        awaitTopic(name);

        topics.updateConfig(name, new TopicConfigUpdateRequest(Map.of("retention.ms", "3600000", "retention.bytes", "52428800"), List.of()));

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            TopicDetailResponse detail = topics.describe(name);
            assertThat(detail.configs().get("retention.ms").value()).isEqualTo("3600000");
            assertThat(detail.configs().get("retention.bytes").value()).isEqualTo("52428800");
        });

        topics.updateConfig(name, new TopicConfigUpdateRequest(Map.of(), List.of("retention.bytes")));

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            var retentionBytes = topics.describe(name).configs().get("retention.bytes");
            assertThat(retentionBytes.value()).isEqualTo("-1");     // vuelve al valor por defecto del broker
            assertThat(retentionBytes.source()).isNotEqualTo("DYNAMIC_TOPIC_CONFIG");
        });
    }

    @Test
    void lasPeticionesDeConfiguracionIncoherentesSeRechazan() {
        String name = unique("t-cfg2");
        topics.create(new TopicRequest(name, 1, (short) 1, Map.of()));
        awaitTopic(name);

        assertThatThrownBy(() -> topics.updateConfig(name, new TopicConfigUpdateRequest(Map.of(), List.of())))
                .isInstanceOf(InvalidConfigurationException.class).hasMessageContaining("al menos una");
        assertThatThrownBy(() -> topics.updateConfig(name,
                new TopicConfigUpdateRequest(Map.of("retention.ms", "1000"), List.of("retention.ms"))))
                .isInstanceOf(InvalidConfigurationException.class).hasMessageContaining("a la vez");
        assertThatThrownBy(() -> topics.updateConfig(name, new TopicConfigUpdateRequest(Map.of("min.insync.replicas", "2"), List.of())))
                .isInstanceOf(UnprocessableOperationException.class);
        assertThatThrownBy(() -> topics.updateConfig(unique("no-existe"), new TopicConfigUpdateRequest(Map.of("retention.ms", "1000"), List.of())))
                .isInstanceOf(ResourceNotFoundException.class);
    }

    @Test
    void aumentaLasParticionesPeroNuncaLasReduce() {
        String name = unique("t-part");
        topics.create(new TopicRequest(name, 2, (short) 1, Map.of()));
        awaitTopic(name);

        assertThat(topics.increasePartitions(name, 4).partitions()).hasSize(4);
        assertThatThrownBy(() -> topics.increasePartitions(name, 2))
                .isInstanceOf(UnprocessableOperationException.class).hasMessageContaining("no permite reducir");
        assertThatThrownBy(() -> topics.increasePartitions(name, 4))
                .isInstanceOf(UnprocessableOperationException.class).hasMessageContaining("ya tiene 4");
    }

    // En Windows un broker Kafka no puede renombrar el directorio de un tópico para borrarlo (el archivo está mapeado en
    // memoria, KAFKA-1194) y apaga su log dir: es del broker embebido, no del servicio. En Linux/Docker corre normal.
    // El borrado también se prueba, con un Admin simulado, en TopicAdminServiceTest.
    @Test
    @DisabledOnOs(OS.WINDOWS)
    void eliminaUnTopicoYLosInternosEstanProtegidos() {
        String name = unique("t-del");
        topics.create(new TopicRequest(name, 1, (short) 1, Map.of()));
        awaitTopic(name);

        topics.delete(name);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(topics.list(false)).extracting("name").doesNotContain(name));
        assertThatThrownBy(() -> topics.describe(name)).isInstanceOf(ResourceNotFoundException.class).hasMessageContaining(name);
        assertThatThrownBy(() -> topics.delete(name)).isInstanceOf(ResourceNotFoundException.class);
        assertThatThrownBy(() -> topics.delete("__consumer_offsets")).isInstanceOf(UnprocessableOperationException.class);
    }

    // ---------- Aprovisionamiento ----------

    @Test
    void aprovisionaLaTopologiaDelProyectoYEsIdempotente() {
        ProvisionResponse first = provisioning.provision();

        assertThat(first.brokers()).isEqualTo(1);
        assertThat(first.topics()).extracting("topic")
                .containsExactly("bookings.events", "audit.timeline", "bookings.events.DLT", "audit.timeline.DLT");
        assertThat(first.created() + first.updated() + first.unchanged()).isEqualTo(4);
        // el factor 3 del proyecto no cabe en un clúster de 1 broker: se avisa (al crear lo ajusta; si ya existía, lo informa)
        assertThat(first.topics().get(0).warnings()).anyMatch(w -> w.contains("replicación") && w.contains("3") && w.contains("1"));
        awaitTopic("audit.timeline");

        TopicDetailResponse timeline = topics.describe("audit.timeline");
        assertThat(timeline.partitions()).hasSize(3);
        assertThat(timeline.configs().get("cleanup.policy").value()).isEqualTo("compact,delete");
        assertThat(timeline.configs().get("retention.ms").value()).isEqualTo("2592000000");
        assertThat(timeline.configs().get("min.insync.replicas").value()).isEqualTo("1");   // ajustado a las réplicas reales

        TopicDetailResponse events = topics.describe("bookings.events");
        assertThat(events.configs().get("cleanup.policy").value()).isEqualTo("delete");
        assertThat(events.configs().get("retention.ms").value()).isEqualTo("604800000");

        // segunda ejecución: no hay nada que cambiar
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            ProvisionResponse again = provisioning.provision();
            assertThat(again.created()).isZero();
            assertThat(again.updated()).isZero();
            assertThat(again.unchanged()).isEqualTo(4);
        });
    }

    @Test
    void aprovisionarAlineaLaConfiguracionDeUnTopicoQueSeDesvio() {
        provisioning.provision();
        awaitTopic("bookings.events");
        topics.updateConfig("bookings.events", new TopicConfigUpdateRequest(Map.of("retention.ms", "1000"), List.of()));
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(topics.describe("bookings.events").configs().get("retention.ms").value()).isEqualTo("1000"));

        ProvisionResponse response = provisioning.provision();

        assertThat(response.updated()).isEqualTo(1);
        var result = response.topics().stream().filter(t -> t.topic().equals("bookings.events")).findFirst().orElseThrow();
        assertThat(result.action()).isEqualTo("UPDATED");
        assertThat(result.notes()).anyMatch(n -> n.contains("retention.ms: 1000 -> 604800000"));
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(topics.describe("bookings.events").configs().get("retention.ms").value()).isEqualTo("604800000"));
    }

    // ---------- Consumer groups ----------

    @Test
    void unGrupoInactivoMuestraSuLagYSePuedeReiniciarYEliminar() {
        String topic = unique("t-grp");
        String group = unique("g-inactivo");
        topics.create(new TopicRequest(topic, 1, (short) 1, Map.of()));
        awaitTopic(topic);
        produce(topic, 5);

        // el grupo lee 3 de 5 mensajes, confirma y se desconecta
        consumeAndCommit(topic, group, 3);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            ConsumerGroupDetail detail = groups.describe(group);
            assertThat(detail.state()).isEqualTo("EMPTY");
            assertThat(detail.members()).isEmpty();
            assertThat(detail.offsets()).hasSize(1);
            assertThat(detail.offsets().get(0).committedOffset()).isEqualTo(3);
            assertThat(detail.offsets().get(0).endOffset()).isEqualTo(5);
            assertThat(detail.offsets().get(0).lag()).isEqualTo(2);
            assertThat(detail.totalLag()).isEqualTo(2);
        });
        assertThat(groups.list()).anyMatch(g -> g.groupId().equals(group) && g.state().equals("EMPTY"));

        // simulación: no cambia nada
        ResetOffsetsResponse dry = groups.resetOffsets(group,
                new ResetOffsetsRequest(topic, OffsetResetStrategy.EARLIEST, null, null, List.of(), true));
        assertThat(dry.dryRun()).isTrue();
        assertThat(dry.applied()).isFalse();
        assertThat(dry.changes()).singleElement().satisfies(c -> {
            assertThat(c.from()).isEqualTo(3L);
            assertThat(c.to()).isZero();
        });
        assertThat(groups.describe(group).totalLag()).isEqualTo(2);

        // al principio: el grupo vuelve a tener los 5 pendientes
        groups.resetOffsets(group, new ResetOffsetsRequest(topic, OffsetResetStrategy.EARLIEST, null, null, List.of(), false));
        assertThat(groups.describe(group).totalLag()).isEqualTo(5);

        // a un offset concreto
        groups.resetOffsets(group, new ResetOffsetsRequest(topic, OffsetResetStrategy.OFFSET, 4L, null, List.of(0), false));
        assertThat(groups.describe(group).totalLag()).isEqualTo(1);

        // al final
        groups.resetOffsets(group, new ResetOffsetsRequest(topic, OffsetResetStrategy.LATEST, null, null, List.of(), false));
        assertThat(groups.describe(group).totalLag()).isZero();

        // a un instante: antes de todo = principio; en el futuro = final
        groups.resetOffsets(group, new ResetOffsetsRequest(topic, OffsetResetStrategy.TIMESTAMP, null,
                Instant.now().minusSeconds(3600), List.of(), false));
        assertThat(groups.describe(group).totalLag()).isEqualTo(5);
        groups.resetOffsets(group, new ResetOffsetsRequest(topic, OffsetResetStrategy.TIMESTAMP, null,
                Instant.now().plusSeconds(3600), List.of(), false));
        assertThat(groups.describe(group).totalLag()).isZero();

        // validaciones
        assertThatThrownBy(() -> groups.resetOffsets(group,
                new ResetOffsetsRequest(topic, OffsetResetStrategy.OFFSET, 99L, null, List.of(), false)))
                .isInstanceOf(UnprocessableOperationException.class).hasMessageContaining("fuera del rango");
        assertThatThrownBy(() -> groups.resetOffsets(group,
                new ResetOffsetsRequest(topic, OffsetResetStrategy.OFFSET, null, null, List.of(), false)))
                .isInstanceOf(InvalidConfigurationException.class).hasMessageContaining("offset");
        assertThatThrownBy(() -> groups.resetOffsets(group,
                new ResetOffsetsRequest(topic, OffsetResetStrategy.EARLIEST, null, null, List.of(7), false)))
                .isInstanceOf(InvalidConfigurationException.class).hasMessageContaining("partición 7");
        assertThatThrownBy(() -> groups.resetOffsets(group,
                new ResetOffsetsRequest(unique("no-existe"), OffsetResetStrategy.EARLIEST, null, null, List.of(), false)))
                .isInstanceOf(ResourceNotFoundException.class);

        // eliminar
        groups.delete(group);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThatThrownBy(() -> groups.describe(group)).isInstanceOf(ResourceNotFoundException.class));
    }

    @Test
    void unGrupoConConsumidoresActivosMuestraMiembrosYNoAdmiteReiniciarNiEliminar() {
        String topic = unique("t-activo");
        String group = unique("g-activo");
        topics.create(new TopicRequest(topic, 2, (short) 1, Map.of()));
        awaitTopic(topic);
        produce(topic, 4);

        try (KafkaConsumer<String, String> consumer = consumer(group)) {
            consumer.subscribe(List.of(topic));
            await().atMost(Duration.ofSeconds(30)).until(() -> {
                consumer.poll(Duration.ofMillis(200));
                return !consumer.assignment().isEmpty();
            });
            consumer.commitSync();

            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                ConsumerGroupDetail detail = groups.describe(group);
                assertThat(detail.state()).isEqualTo("STABLE");
                assertThat(detail.members()).hasSize(1);
                assertThat(detail.members().get(0).assignedPartitions()).hasSize(2);
                assertThat(detail.coordinatorId()).isNotNull();
            });

            assertThatThrownBy(() -> groups.resetOffsets(group,
                    new ResetOffsetsRequest(topic, OffsetResetStrategy.EARLIEST, null, null, List.of(), false)))
                    .isInstanceOf(ResourceConflictException.class).hasMessageContaining("STABLE");
            assertThatThrownBy(() -> groups.delete(group)).isInstanceOf(ResourceConflictException.class);
        }
    }

    @Test
    void unGrupoQueNoExisteEsUn404() {
        assertThatThrownBy(() -> groups.describe(unique("fantasma"))).isInstanceOf(ResourceNotFoundException.class);
        assertThatThrownBy(() -> groups.delete(unique("fantasma"))).isInstanceOf(ResourceNotFoundException.class);
    }

    // ---------- Inspección de mensajes ----------

    @Test
    void veLosUltimosMensajesDeUnTopicoConSusHeadersSinCrearUnGrupo() {
        String topic = unique("t-msg");
        topics.create(new TopicRequest(topic, 1, (short) 1, Map.of()));
        awaitTopic(topic);
        try (KafkaProducer<String, String> producer = producer()) {
            for (int i = 1; i <= 5; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, "k" + i, "mensaje-" + i);
                record.headers().add(new RecordHeader("kafka_dlt-exception-message", ("causa " + i).getBytes()));
                producer.send(record);
            }
            producer.flush();
        }
        int groupsBefore = groups.list().size();

        List<MessageView> latest = inspector.latest(topic, 3);

        assertThat(latest).extracting(MessageView::value).containsExactly("mensaje-5", "mensaje-4", "mensaje-3");
        assertThat(latest.get(0).key()).isEqualTo("k5");
        assertThat(latest.get(0).offset()).isEqualTo(4);
        assertThat(latest.get(0).headers()).containsEntry("kafka_dlt-exception-message", "causa 5");
        assertThat(groups.list()).hasSize(groupsBefore);
        assertThat(inspector.latest(topic, 100)).hasSize(5);
    }

    @Test
    void inspeccionarUnTopicoVacioODesconocido() {
        String empty = unique("t-vacio");
        topics.create(new TopicRequest(empty, 1, (short) 1, Map.of()));
        awaitTopic(empty);

        assertThat(inspector.latest(empty, 10)).isEmpty();
        assertThatThrownBy(() -> inspector.latest(unique("no-existe"), 10)).isInstanceOf(ResourceNotFoundException.class);
        assertThatThrownBy(() -> inspector.latest(empty, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    // ---------- helpers ----------

    private void awaitTopic(String name) {
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(topics.describe(name).partitions()).isNotEmpty());
    }

    private void produce(String topic, int count) {
        try (KafkaProducer<String, String> producer = producer()) {
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(topic, 0, "k", "v" + i));
            }
            producer.flush();
        }
    }

    private KafkaProducer<String, String> producer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<String, String> consumer(String group) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }

    /** Lee exactamente {@code count} mensajes, confirma el offset y se desconecta (el grupo queda EMPTY). */
    private void consumeAndCommit(String topic, String group, int count) {
        try (KafkaConsumer<String, String> consumer = consumer(group)) {
            consumer.assign(List.of(new org.apache.kafka.common.TopicPartition(topic, 0)));
            consumer.seekToBeginning(consumer.assignment());
            int read = 0;
            long deadline = System.currentTimeMillis() + 20_000;
            while (read < count && System.currentTimeMillis() < deadline) {
                var records = consumer.poll(Duration.ofMillis(300));
                for (var record : records) {
                    if (read == count) break;
                    read++;
                    consumer.commitSync(Map.of(new org.apache.kafka.common.TopicPartition(topic, 0),
                            new org.apache.kafka.clients.consumer.OffsetAndMetadata(record.offset() + 1)));
                }
            }
            assertThat(read).isEqualTo(count);
        }
    }
}
