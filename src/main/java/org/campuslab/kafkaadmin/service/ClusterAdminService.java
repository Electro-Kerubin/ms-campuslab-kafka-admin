package org.campuslab.kafkaadmin.service;

import java.util.Comparator;
import java.util.List;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import org.campuslab.kafkaadmin.dto.BrokerInfo;
import org.campuslab.kafkaadmin.dto.ClusterResponse;
import org.springframework.stereotype.Service;

/** Estado del clúster: cuántos brokers responden y cuál es el controlador. */
@Service
public class ClusterAdminService {

    private final Admin admin;
    private final KafkaErrorTranslator kafka;

    public ClusterAdminService(Admin admin, KafkaErrorTranslator kafka) {
        this.admin = admin;
        this.kafka = kafka;
    }

    public ClusterResponse describe() {
        DescribeClusterResult cluster = admin.describeCluster();
        String id = kafka.await(cluster.clusterId(), "consultar el clúster", "clúster");
        Node controller = kafka.await(cluster.controller(), "consultar el clúster", "clúster");
        List<BrokerInfo> brokers = kafka.await(cluster.nodes(), "consultar el clúster", "clúster").stream()
                .map(node -> new BrokerInfo(node.id(), node.host(), node.port(), node.rack()))
                .sorted(Comparator.comparingInt(BrokerInfo::id))
                .toList();
        // En modo KRaft el controlador no es un broker concreto y puede venir vacío
        return new ClusterResponse(id, controller == null || controller.isEmpty() ? null : controller.id(), brokers);
    }
}
