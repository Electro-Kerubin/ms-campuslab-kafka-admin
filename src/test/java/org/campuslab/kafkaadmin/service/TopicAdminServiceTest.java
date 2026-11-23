package org.campuslab.kafkaadmin.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.campuslab.kafkaadmin.config.KafkaConnectionProperties;
import org.campuslab.kafkaadmin.exception.BrokerUnavailableException;
import org.campuslab.kafkaadmin.exception.ResourceNotFoundException;
import org.campuslab.kafkaadmin.exception.UnprocessableOperationException;
import org.junit.jupiter.api.Test;

/**
 * El borrado de tópicos (que no se puede probar con el broker embebido en Windows, ver KafkaAdminBrokerTest)
 * y el comportamiento cuando el clúster no responde.
 */
class TopicAdminServiceTest {

    private final Admin admin = mock(Admin.class);
    private final KafkaConnectionProperties connection = new KafkaConnectionProperties("localhost:9092", 1000, Map.of());
    private final KafkaErrorTranslator kafka = new KafkaErrorTranslator(connection);
    private final TopicAdminService service = new TopicAdminService(admin, kafka, new OffsetLookup(admin, kafka));

    private static TopicDescription description(String name) {
        Node node = new Node(1, "localhost", 9092);
        return new TopicDescription(name, false, List.of(new TopicPartitionInfo(0, node, List.of(node), List.of(node))));
    }

    private void topicExists(String name) {
        DescribeTopicsResult described = mock(DescribeTopicsResult.class);
        when(described.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of(name, description(name))));
        when(admin.describeTopics(List.of(name))).thenReturn(described);
    }

    @Test
    void eliminaElTopicoCuandoExiste() {
        topicExists("t-viejo");
        DeleteTopicsResult deleted = mock(DeleteTopicsResult.class);
        when(deleted.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.deleteTopics(List.of("t-viejo"))).thenReturn(deleted);

        service.delete("t-viejo");

        verify(admin).deleteTopics(List.of("t-viejo"));
    }

    @Test
    void eliminarUnTopicoInexistenteEsUn404YNoIntentaBorrarNada() {
        KafkaFutureImpl<Map<String, TopicDescription>> missing = new KafkaFutureImpl<>();
        missing.completeExceptionally(new UnknownTopicOrPartitionException("no está"));
        DescribeTopicsResult described = mock(DescribeTopicsResult.class);
        when(described.allTopicNames()).thenReturn(missing);
        when(admin.describeTopics(List.of("fantasma"))).thenReturn(described);

        assertThatThrownBy(() -> service.delete("fantasma"))
                .isInstanceOf(ResourceNotFoundException.class).hasMessage("No existe el tópico 'fantasma'");
        verify(admin, never()).deleteTopics(List.of("fantasma"));
    }

    @Test
    void losTopicosInternosNoSeEliminanNiSiquieraConsultandoElCluster() {
        assertThatThrownBy(() -> service.delete("__consumer_offsets"))
                .isInstanceOf(UnprocessableOperationException.class).hasMessageContaining("interno");
        assertThatThrownBy(() -> service.delete("__transaction_state"))
                .isInstanceOf(UnprocessableOperationException.class);
        verify(admin, never()).deleteTopics(List.of("__consumer_offsets"));
    }

    @Test
    void sinBrokersLasOperacionesFallanRapidoConServicioNoDisponible() {
        KafkaConnectionProperties unreachable = new KafkaConnectionProperties("localhost:1", 1500, Map.of());
        try (Admin real = Admin.create(unreachable.clientProperties())) {
            KafkaErrorTranslator translator = new KafkaErrorTranslator(unreachable);
            TopicAdminService offline = new TopicAdminService(real, translator, new OffsetLookup(real, translator));

            long start = System.nanoTime();
            assertThatThrownBy(() -> offline.list(false)).isInstanceOf(BrokerUnavailableException.class);
            assertThat((System.nanoTime() - start) / 1_000_000).isLessThan(10_000);
        }
    }
}
