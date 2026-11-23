package org.campuslab.kafkaadmin.config;

import org.apache.kafka.clients.admin.Admin;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Un único AdminClient compartido por todos los servicios (es seguro entre hilos); se cierra al apagar la app. */
@Configuration
public class KafkaClientConfig {

    @Bean(destroyMethod = "close")
    public Admin kafkaAdminClient(KafkaConnectionProperties connection) {
        return Admin.create(connection.clientProperties());
    }
}
