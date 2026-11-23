package org.campuslab.kafkaadmin.service;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;

import org.campuslab.kafkaadmin.exception.InvalidConfigurationException;

/**
 * Qué configuraciones de tópico se pueden administrar por la API y qué valores admiten.
 * Es una lista blanca: una clave desconocida o un valor fuera de rango se rechaza ANTES de llegar al clúster,
 * con un mensaje que dice qué se esperaba (Kafka respondería con errores genéricos).
 */
public final class TopicConfigRules {

    private record Rule(String expected, Predicate<String> valid) {
    }

    private static final Set<String> COMPRESSION = Set.of("producer", "uncompressed", "gzip", "snappy", "lz4", "zstd");
    private static final Set<String> CLEANUP = Set.of("delete", "compact", "compact,delete", "delete,compact");

    /** TreeMap: los mensajes de error listan las claves en orden alfabético. */
    private static final Map<String, Rule> RULES = new TreeMap<>(Map.ofEntries(
            Map.entry("cleanup.policy", new Rule("delete, compact o compact,delete", CLEANUP::contains)),
            Map.entry("retention.ms", new Rule("milisegundos >= 0, o -1 para no borrar por tiempo", v -> longAtLeast(v, -1))),
            Map.entry("retention.bytes", new Rule("bytes por partición >= 0, o -1 sin límite", v -> longAtLeast(v, -1))),
            Map.entry("min.insync.replicas", new Rule("entero >= 1", v -> longAtLeast(v, 1))),
            Map.entry("segment.bytes", new Rule("entero >= 14", v -> longAtLeast(v, 14))),
            Map.entry("segment.ms", new Rule("milisegundos >= 1", v -> longAtLeast(v, 1))),
            Map.entry("max.message.bytes", new Rule("entero >= 0", v -> longAtLeast(v, 0))),
            Map.entry("delete.retention.ms", new Rule("milisegundos >= 0", v -> longAtLeast(v, 0))),
            Map.entry("min.cleanable.dirty.ratio", new Rule("número entre 0 y 1", TopicConfigRules::ratio)),
            Map.entry("compression.type", new Rule("producer, uncompressed, gzip, snappy, lz4 o zstd", COMPRESSION::contains)),
            Map.entry("message.timestamp.type", new Rule("CreateTime o LogAppendTime",
                    v -> v.equals("CreateTime") || v.equals("LogAppendTime"))),
            Map.entry("unclean.leader.election.enable", new Rule("true o false", v -> v.equals("true") || v.equals("false")))
    ));

    private TopicConfigRules() {
    }

    public static Set<String> allowedKeys() {
        return RULES.keySet();
    }

    /** Valida clave por clave; lanza InvalidConfigurationException con el primer problema encontrado. */
    public static void validate(Map<String, String> configs) {
        configs.forEach((key, value) -> {
            Rule rule = ruleFor(key);
            if (value == null || !rule.valid().test(value.trim())) {
                throw new InvalidConfigurationException(
                        "Valor inválido para " + key + ": '" + value + "'. Se espera: " + rule.expected());
            }
        });
    }

    /** Una clave que se va a devolver a su valor por defecto también debe ser una de las administrables. */
    public static void validateKey(String key) {
        ruleFor(key);
    }

    private static Rule ruleFor(String key) {
        Rule rule = key == null ? null : RULES.get(key);
        if (rule == null) {
            throw new InvalidConfigurationException(
                    "La configuración '" + key + "' no se puede administrar. Permitidas: " + String.join(", ", RULES.keySet()));
        }
        return rule;
    }

    private static boolean longAtLeast(String value, long min) {
        try {
            return Long.parseLong(value) >= min;
        } catch (NumberFormatException notANumber) {
            return false;
        }
    }

    private static boolean ratio(String value) {
        try {
            double parsed = Double.parseDouble(value);
            return parsed >= 0 && parsed <= 1;
        } catch (NumberFormatException notANumber) {
            return false;
        }
    }
}
