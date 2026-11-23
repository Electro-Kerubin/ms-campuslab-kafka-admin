package org.campuslab.kafkaadmin.dto;

import java.util.List;

/** Estado del clúster: identificador, broker que hace de controlador y brokers vivos. */
public record ClusterResponse(String clusterId, Integer controllerId, List<BrokerInfo> brokers) {
}
