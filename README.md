# ms-campuslab-kafka-admin

Microservicio **administrador de Kafka** de CampusLab (el equivalente de `ms-campuslab-mq-admin` para streaming). Expone una API
REST para **crear, listar, describir, modificar y eliminar tópicos y sus configuraciones**, aprovisionar la topología del proyecto
y **administrar consumer groups** (estado, lag, reinicio de offsets). No publica ni consume mensajes de negocio.

- Spring Boot 3.3.3 · Java 21 · `kafka-clients` (`AdminClient`)
- Seguridad: JWT de Azure AD, **solo rol `ADMIN`** (igual que `bookings`, `catalog` y `mq-admin`)
- Documentación interactiva: **http://localhost:8087/swagger-ui.html** (OpenAPI en `/v3/api-docs`)

## Estructura

```
controller/   TopicController · ConsumerGroupController · ClusterController     (REST, no conocen Kafka)
service/      TopicAdminService          crear · listar · describir · config · particiones · eliminar
              TopicProvisioningService   aprovisiona la topología del proyecto (idempotente)
              ConsumerGroupAdminService  estado · lag · reinicio de offsets · eliminar
              ClusterAdminService        brokers y controlador
              MessageInspector           últimos mensajes de un tópico (solo lectura; sirve para revisar las DLT)
              OffsetLookup               offsets de las particiones (primero, último, por fecha)
              TopicConfigRules           lista blanca de configuraciones y sus rangos
              KafkaErrorTranslator       errores de Kafka -> excepciones de dominio -> HTTP
dto/          TopicRequest · TopicConfigUpdateRequest · PartitionsRequest · ResetOffsetsRequest · *Response
exception/    ResourceNotFound · ResourceConflict · InvalidConfiguration · UnprocessableOperation
              BrokerUnavailable · BrokerOperation · GlobalExceptionHandler      (formato de error único)
config/       KafkaConnectionProperties (brokers) · ManagedTopicsProperties (topología) · KafkaClientConfig · OpenApiConfig
security/     SecurityConfig                                                    (JWT + roles)
```

## Endpoints

Base: `/api/kafka` · todos requieren `Authorization: Bearer <JWT>` con el rol `ADMIN`.

### Tópicos

| Verbo | Ruta | Qué hace | Éxito | Errores |
|---|---|---|---|---|
| `GET` | `/topics?includeInternal=false` | Lista (nombre, particiones, factor de replicación) | `200` | `503` |
| `POST` | `/topics` | Crea un tópico (por defecto 3 particiones, RF 3) | `201` + `Location` | `400` datos/config inválidos · `409` ya existe · `422` más réplicas que brokers |
| `POST` | `/topics/provision` | Crea/alinea la topología del proyecto (idempotente) | `200` | `503` |
| `GET` | `/topics/{name}` | Particiones (líder, réplicas, ISR, offsets, mensajes) y configuración efectiva | `200` | `404` |
| `PATCH` | `/topics/{name}/config` | Fija (`set`) o devuelve a su valor por defecto (`reset`) configuraciones | `200` | `400` · `404` · `422` |
| `PUT` | `/topics/{name}/partitions` | Aumenta las particiones (Kafka no permite reducirlas) | `200` | `400` · `404` · `422` |
| `GET` | `/topics/{name}/messages?limit=20` | Últimos mensajes de cada partición con sus headers (no crea un grupo ni mueve offsets) | `200` | `400` · `404` |
| `DELETE` | `/topics/{name}` | Elimina el tópico (los internos `__*` están protegidos) | `204` | `404` · `422` |

Configuraciones administrables: `cleanup.policy`, `retention.ms`, `retention.bytes`, `min.insync.replicas`, `segment.bytes`, `segment.ms`,
`max.message.bytes`, `delete.retention.ms`, `min.cleanable.dirty.ratio`, `compression.type`, `message.timestamp.type`,
`unclean.leader.election.enable`. Una clave desconocida o un valor fuera de rango se rechaza antes de llegar al clúster con un mensaje
que dice qué se esperaba. `min.insync.replicas` nunca puede superar el factor de replicación (ningún productor con `acks=all` podría escribir).

### Consumer groups

| Verbo | Ruta | Qué hace | Éxito | Errores |
|---|---|---|---|---|
| `GET` | `/consumer-groups` | Lista los grupos con su estado (`STABLE`, `EMPTY`, `PREPARING_REBALANCE`...) | `200` | `503` |
| `GET` | `/consumer-groups/{groupId}` | Miembros con sus particiones asignadas y, por partición, offset confirmado, último offset y **lag** | `200` | `404` |
| `POST` | `/consumer-groups/{groupId}/offsets` | Reinicia los offsets: `EARLIEST`, `LATEST`, `OFFSET`, `TIMESTAMP`; `dryRun` solo simula | `200` | `400` · `404` · `409` grupo activo · `422` offset fuera de rango |
| `DELETE` | `/consumer-groups/{groupId}` | Elimina el grupo y sus offsets | `204` | `404` · `409` grupo activo |

### Clúster

| Verbo | Ruta | Qué hace |
|---|---|---|
| `GET` | `/cluster` | Identificador, controlador y brokers que responden |

Sin token `401`; con token sin rol `ADMIN` `403`; clúster caído `503` (falla en segundos, no cuelga). Errores del cliente que detecta
Spring: tipo de contenido que no es JSON `415`, verbo no permitido `405`, ruta inexistente `404` (nunca un `500`).

## Aprovisionar la topología

`POST /api/kafka/topics/provision` deja el clúster con la tabla de `application.yml` (`campuslab.kafka-admin.topics`, CONTEXT.md §9):

| Tópico | Particiones | Réplicas | `cleanup.policy` | Retención |
|---|---|---|---|---|
| `bookings.events` | 3 | 3 | `delete` | 7 días |
| `audit.timeline` | 3 | 3 | `compact,delete` | 30 días |
| `bookings.events.DLT` | 3 | 3 | `delete` | 14 días |
| `audit.timeline.DLT` | 3 | 3 | `delete` | 14 días |

Es **idempotente y replicable**: en un clúster vacío crea todo; en uno ya aprovisionado no cambia nada (`UNCHANGED`); si alguien cambió una
configuración a mano, la vuelve a alinear (`UPDATED`) e informa qué cambió. Lo que Kafka no permite (reducir particiones, cambiar el factor
de replicación) lo informa como advertencia en vez de fallar. Con `adapt-replication-to-cluster: true` (por defecto), si el clúster tiene
menos brokers que el RF pedido (p. ej. el Kafka de desarrollo de 1 broker) se crea con tantas réplicas como brokers haya y lo avisa.

El mismo resultado se logra con los scripts de `campuslab-infra/kafka/scripts` (`create-topics.sh`, `topics.sh`, `groups.sh`).

## Ejemplos

```bash
TOKEN=<JWT con rol ADMIN>
H="Authorization: Bearer $TOKEN"

# Dejar el clúster con la topología del proyecto
curl -X POST localhost:8087/api/kafka/topics/provision -H "$H"

# Crear un tópico propio con retención de 1 hora
curl -X POST localhost:8087/api/kafka/topics -H "$H" -H 'Content-Type: application/json' \
     -d '{"name":"pruebas","partitions":3,"replicationFactor":3,"configs":{"retention.ms":"3600000","cleanup.policy":"delete"}}'

# Cambiar la retención de bookings.events a 3 días y subirle las particiones a 6
curl -X PATCH localhost:8087/api/kafka/topics/bookings.events/config -H "$H" -H 'Content-Type: application/json' \
     -d '{"set":{"retention.ms":"259200000"}}'
curl -X PUT localhost:8087/api/kafka/topics/bookings.events/partitions -H "$H" -H 'Content-Type: application/json' \
     -d '{"partitions":6}'

# Ver el estado de un tópico y el lag de un grupo
curl localhost:8087/api/kafka/topics/bookings.events -H "$H"
curl localhost:8087/api/kafka/consumer-groups/audit-group -H "$H"

# Revisar qué cayó en la DLT y por qué (headers kafka_dlt-exception-message / kafka_dlt-original-topic)
curl "localhost:8087/api/kafka/topics/bookings.events.DLT/messages?limit=10" -H "$H"

# Reprocesar desde el principio con el grupo report-group (detener antes sus consumidores): primero en simulación
curl -X POST localhost:8087/api/kafka/consumer-groups/report-group/offsets -H "$H" -H 'Content-Type: application/json' \
     -d '{"topic":"bookings.events","strategy":"EARLIEST","dryRun":true}'
```

## Configuración

| Variable | Por defecto | Para qué |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Brokers separados por coma (clúster del proyecto: `localhost:9092,localhost:9093,localhost:9094`) |
| `KAFKA_REQUEST_TIMEOUT_MS` | `8000` | Tiempo máximo de cada operación; si se supera, `503` |
| `KAFKA_ADAPT_REPLICATION` | `true` | Ajustar el RF al número de brokers al aprovisionar |
| `TENANT_ID` | `changeme` | Tenant de Azure AD para validar el JWT |
| `SERVER_PORT` | `8087` | Puerto HTTP |

## Seguridad e IDaaS (Azure AD)

Valida el JWT igual que `mq-admin`, `bookings` y `catalog`: firma y emisor `https://login.microsoftonline.com/${TENANT_ID}/v2.0`, y el claim `roles`
(solo `ADMIN` administra; `TECNICO`/`ESTUDIANTE`/`AUDITOR` reciben `403`; sin token, `401`). En uso real: `TENANT_ID=7da49cdc-96c1-4342-bf8b-54142c0c27d8`
(el tenant de la App Registration "CampusLab") y un usuario con el App Role `ADMIN` asignado. Como administra la infraestructura, **no se publica por el BFF
ni por API Management**: se opera directo. Para la demostración sin un usuario ADMIN de Azure, `campuslab-infra/kafka/demo` usa el emisor de tokens local
(`mq/demo/auth-local.js`): el mismo código de validación con otra fuente de claves públicas.

Para un clúster en la nube con seguridad, los parámetros del cliente (`security.protocol`, `sasl.*`, `ssl.*`) van en
`campuslab.kafka.properties` de `application.yml`.

## Ejecutar y probar

```bash
mvn spring-boot:run            # requiere un Kafka accesible y TENANT_ID; el JWT lo emite Azure AD
mvn test                       # no requiere Docker: usa un broker Kafka embebido
docker build -t ms-campuslab-kafka-admin .
```

Los tests corren los servicios contra un **broker Kafka real embebido** (`spring-kafka-test`): crear/describir/modificar tópicos,
aprovisionamiento idempotente, consumer groups con lag y reinicio de offsets, inspección de mensajes. En Windows se omite solo el borrado
de tópicos con broker real (limitación conocida de Kafka en Windows, KAFKA-1194); el borrado se prueba con un `Admin` simulado.
# ms-campuslab-kafka-admin
