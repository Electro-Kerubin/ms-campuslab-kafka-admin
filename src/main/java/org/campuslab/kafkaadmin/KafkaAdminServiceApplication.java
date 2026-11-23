package org.campuslab.kafkaadmin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Kafka Admin Service Application
 *
 * Microservicio administrador de Kafka (el equivalente de ms-campuslab-mq-admin para streaming).
 *
 * Responsabilidades:
 * - Crear, listar, describir, modificar (configuración y particiones) y eliminar tópicos (/api/kafka/topics)
 * - Aprovisionar la topología del proyecto de forma idempotente (bookings.events, audit.timeline y sus DLT)
 * - Inspeccionar consumer groups: estado, miembros, offsets y lag; reiniciar offsets y eliminar grupos
 * - Mirar los últimos mensajes de un tópico (útil para revisar las DLT)
 *
 * No publica ni consume mensajes de negocio, y solo lo opera el rol ADMIN.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class KafkaAdminServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaAdminServiceApplication.class, args);
    }

}
