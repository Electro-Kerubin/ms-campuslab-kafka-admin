package org.campuslab.kafkaadmin.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.campuslab.kafkaadmin.exception.InvalidConfigurationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Qué configuraciones de tópico y qué valores acepta la API antes de tocar el clúster. */
class TopicConfigRulesTest {

    @Test
    void aceptaLaConfiguracionDeLaTopologiaDelProyecto() {
        assertThatCode(() -> TopicConfigRules.validate(Map.of(
                "cleanup.policy", "compact,delete",
                "retention.ms", "2592000000",
                "min.insync.replicas", "2",
                "min.cleanable.dirty.ratio", "0.1",
                "segment.ms", "86400000",
                "delete.retention.ms", "86400000",
                "retention.bytes", "-1",
                "compression.type", "snappy"))).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @CsvSource({
            "cleanup.policy,borrar",
            "cleanup.policy,''",
            "retention.ms,-2",
            "retention.ms,una semana",
            "retention.bytes,-5",
            "min.insync.replicas,0",
            "segment.bytes,10",
            "segment.ms,0",
            "max.message.bytes,-1",
            "min.cleanable.dirty.ratio,1.5",
            "min.cleanable.dirty.ratio,mucho",
            "compression.type,rar",
            "message.timestamp.type,Whenever",
            "unclean.leader.election.enable,si"
    })
    void rechazaValoresFueraDeRangoIndicandoQueSeEspera(String key, String value) {
        assertThatThrownBy(() -> TopicConfigRules.validate(Map.of(key, value)))
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining(key)
                .hasMessageContaining("Se espera");
    }

    @Test
    void rechazaUnaClaveQueNoEsAdministrableYListaLasPermitidas() {
        assertThatThrownBy(() -> TopicConfigRules.validate(Map.of("flush.messages", "1")))
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("'flush.messages' no se puede administrar")
                .hasMessageContaining("cleanup.policy")
                .hasMessageContaining("retention.ms");
        assertThatThrownBy(() -> TopicConfigRules.validateKey("inventada")).isInstanceOf(InvalidConfigurationException.class);
        assertThat(TopicConfigRules.allowedKeys()).contains("cleanup.policy", "retention.ms", "retention.bytes", "min.insync.replicas");
    }
}
