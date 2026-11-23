package org.campuslab.kafkaadmin.dto;

import java.util.List;

public record ProvisionResponse(int brokers, int created, int updated, int unchanged, List<TopicProvisionResult> topics) {
}
