# Pedidos360 · Backend

Backend de Pedidos360: cuatro microservicios Java / Spring Boot 3.2 que se comunican de forma asíncrona con **RabbitMQ** (comandos de un pedido) y **Kafka** (streaming de eventos para analítica y sincronización), y validan el **JWT de Azure AD** (IDaaS) en cada petición.

| Microservicio | Puerto | Responsabilidad | Rol en RabbitMQ | Rol en Kafka |
|---|---|---|---|---|
| `productos-catalogo` | 8081 | CRUD de productos, stock y métricas de venta | **Consumidor**: descuenta stock (`pedidos.stock.queue`) | **Productor** de `pedidos360.inventario.actualizado` · **consumidor** de `pedidos360.ventas.confirmadas` (grupo `catalogo-metricas-ventas`) |
| `venta-carrito` | 8082 | Carrito, confirmación de pedidos, tickets | **Productor** de `pedido.creado` y **consumidor** de tickets (`pedidos.ticket.queue`) | **Productor** de `pedidos360.ventas.confirmadas` · **consumidor** de `pedidos360.inventario.actualizado` (grupo `carrito-sync-inventario`) |
| `rabbitmq-admin` | 8083 | Administración del broker RabbitMQ | API REST para crear y eliminar colas, exchanges y bindings | — |
| `kafka-admin` | 8084 | Administración de la plataforma Kafka | — | API REST para crear, modificar y eliminar tópicos y configuraciones, y para inspeccionar y administrar consumer groups |

> **¿Por qué los dos brokers?** RabbitMQ sigue a cargo de lo que *debe hacerse una vez* por pedido (descontar stock, emitir el ticket: colas con ACK/NACK y DLQ). Kafka se agrega para lo que *se publica como hecho y lo leen varios* (analítica de ventas y estado del inventario: log particionado, retención, compactación y consumer groups). La integración RabbitMQ de la EP2 no cambió. El diseño completo está en [docs/kafka/casos-de-uso-kafka.md](../docs/kafka/casos-de-uso-kafka.md) y el formato de cada mensaje en [docs/kafka/modelo-mensajes.md](../docs/kafka/modelo-mensajes.md).

---

## 1. Flujo de mensajería

Cuando el usuario confirma su carrito, `venta-carrito` responde de inmediato (**202 Accepted**) y publica el evento `pedido.creado`. El exchange entrega una copia a cada cola enlazada y cada microservicio hace su parte de forma independiente.

```mermaid
flowchart LR
    F[Frontend Angular] -- "POST /api/carrito/{usuarioId}/confirmar + JWT" --> C[venta-carrito]
    C -- "routing key: pedido.creado" --> X{{"pedidos.exchange (direct)"}}
    X -- "pedido.creado" --> QS[("pedidos.stock.queue")]
    X -- "pedido.creado" --> QT[("pedidos.ticket.queue")]
    QS --> CS["StockPedidoConsumer (productos-catalogo)"]
    QT --> CT["TicketConsumer (venta-carrito)"]
    QS -. "NACK / TTL / cola llena" .-> D{{"pedidos.dlx (direct)"}}
    QT -. "NACK / TTL / cola llena" .-> D
    D -- "pedidos.stock.dead" --> DS[("pedidos.stock.dlq")]
    D -- "pedidos.ticket.dead" --> DT[("pedidos.ticket.dlq")]
    DS --> A1["DlqAuditoriaConsumer (log)"]
    DT --> A2["DlqAuditoriaConsumer (log)"]
```

### Topología

| Exchange | Tipo | Routing key | Cola | Consumidor | A dónde van los fallidos |
|---|---|---|---|---|---|
| `pedidos.exchange` | direct | `pedido.creado` | `pedidos.stock.queue` | `StockPedidoConsumer` (catálogo) | `pedidos.dlx` → `pedidos.stock.dlq` |
| `pedidos.exchange` | direct | `pedido.creado` | `pedidos.ticket.queue` | `TicketConsumer` (carrito) | `pedidos.dlx` → `pedidos.ticket.dlq` |
| `pedidos.dlx` | direct | `pedidos.stock.dead` | `pedidos.stock.dlq` | `DlqAuditoriaConsumer` (catálogo) | — |
| `pedidos.dlx` | direct | `pedidos.ticket.dead` | `pedidos.ticket.dlq` | `DlqAuditoriaConsumer` (carrito) | — |

**Políticas de retención** (argumentos de las colas):

| Cola | `x-message-ttl` | `x-max-length` | Efecto |
|---|---|---|---|
| Colas de trabajo | 60 000 ms | 1 000 | Un pedido sin consumir por 60 s, o que excede el tope, pasa a la DLQ |
| DLQ | 86 400 000 ms (24 h) | — | Los mensajes fallidos se conservan un día para analizarlos |

---

## 2. Configuración centralizada

**Ningún nombre de cola, exchange o routing key está escrito en el código.** Todo se define en `application.yml` de cada microservicio, bajo `pedidos360.rabbitmq`, con la misma estructura:

```yaml
pedidos360:
  rabbitmq:
    exchanges:   { pedidos: pedidos.exchange, dead-letter: pedidos.dlx }
    routing-keys: { pedido-creado: pedido.creado }
    colas:
      stock: { nombre: pedidos.stock.queue, dlq: pedidos.stock.dlq, dlq-routing-key: pedidos.stock.dead }
    retencion:  { ttl-mensaje-ms: 60000, max-mensajes: 1000, ttl-dlq-ms: 86400000 }
    reintentos: { max-intentos: 3, intervalo-inicial-ms: 1000, multiplicador: 2.0 }
```

| Clase | Qué hace |
|---|---|
| `config/RabbitMQProperties` | Lee ese bloque del YAML como un objeto tipado y validado; si falta un nombre, la app no arranca |
| `config/RabbitMQConfig` | Declara los beans `Queue`, `Exchange` y `Binding` de cada caso de uso, con el diagrama de la ruta documentado en el código |
| `@RabbitListener(queues = "${...}")` | Los consumidores leen el nombre de la cola desde el YAML |

**Separación de responsabilidades.** Los servicios de negocio (`ProductoService`, `CarritoService`, `TicketService`) no conocen RabbitMQ. `CarritoService` publica a través de la interfaz `PedidoEventPublisher`; su implementación `RabbitPedidoEventPublisher` está en el paquete `messaging`.

```
<microservicio>/src/main/java/com/pedidos360/<servicio>/
├── config/        RabbitMQConfig, RabbitMQProperties, SecurityConfig
├── controller/    REST
├── service/       logica de negocio (sin RabbitMQ)
├── dto/           PedidoCreadoEvent (contrato del mensaje)
└── messaging/
    ├── pedidos/   productor / consumidor del dominio "pedidos"   (StockPedidoConsumer, RabbitPedidoEventPublisher)
    ├── tickets/   consumidor del dominio "tickets"               (TicketConsumer)
    ├── dlq/       auditoria de mensajes muertos                  (DlqAuditoriaConsumer)
    └── comun/     LectorMensajes, EjecutorConReintentos, ClasificadorErrores
```

---

## 3. Confirmación de mensajes (ACK / NACK) y manejo de errores

Los consumidores trabajan con `acknowledge-mode: manual` y deciden de forma explícita qué responderle al broker:

| Situación | Ejemplo | ¿Se reintenta? | Respuesta al broker | Resultado |
|---|---|---|---|---|
| Procesado correctamente | Stock descontado | — | `basicAck` | Sale de la cola |
| Mensaje inválido | JSON roto, falta `pedidoId`, lista vacía | **No** | `basicNack(requeue=false)` | DLQ inmediata |
| Regla de negocio | Producto inexistente, stock insuficiente | **No** | `basicNack(requeue=false)` | DLQ inmediata |
| Error recuperable | BD caída, timeout | **Sí**: 3 intentos (espera 1 s y 2 s) | `basicAck` si se recupera; si no, `basicNack(requeue=false)` | DLQ tras 3 intentos |
| Servicio detenido en medio de un reintento | Apagado de la app | — | `basicNack(requeue=true)` | Vuelve a la cola para otra instancia |

- **Nunca se reencola un mensaje que falló por sus datos**: volvería a fallar para siempre (un *poison message*).
- `ClasificadorErrores` decide qué es recuperable y `EjecutorConReintentos` aplica el backoff exponencial configurado en el YAML.
- **Todo mensaje que llega a la DLQ queda en el log**: el consumidor lo registra al rechazarlo y `DlqAuditoriaConsumer` registra la cola de origen y el motivo según el header `x-death` (`rejected`, `expired` o `maxlen`).
- **El productor también registra los mensajes no entregados**: con *publisher confirms* y `mandatory=true`, un mensaje que el broker rechaza o que no llega a ninguna cola queda en el log (`ConfirmCallback` / `ReturnsCallback`).

---

## 4. API del administrador de RabbitMQ (`rabbitmq-admin`, puerto 8083)

Documentación interactiva: **http://localhost:8083/swagger-ui.html**

| Método | Ruta | Descripción | Respuestas |
|---|---|---|---|
| POST | `/api/rabbitmq/queues` | Crear cola (opcional: DLQ, TTL, máx. mensajes, quorum) | 201, 400, 409 |
| GET | `/api/rabbitmq/queues/{nombre}` | Mensajes pendientes y consumidores | 200, 404 |
| DELETE | `/api/rabbitmq/queues/{nombre}` | Eliminar cola | 204, 404 |
| POST | `/api/rabbitmq/exchanges` | Crear exchange (DIRECT, TOPIC, FANOUT, HEADERS) | 201, 400, 409 |
| DELETE | `/api/rabbitmq/exchanges/{nombre}` | Eliminar exchange | 204, 404 |
| POST | `/api/rabbitmq/bindings` | Enlazar cola ↔ exchange con una routing key | 201, 400, 404 |
| DELETE | `/api/rabbitmq/bindings?exchange=&cola=&routingKey=` | Eliminar binding | 204, 404 |

Ejemplo:

```http
POST /api/rabbitmq/queues
{ "nombre": "notificaciones.email.queue", "conDlq": true, "ttlMs": 60000, "maxMensajes": 500 }

201 Created
{ "mensaje": "Cola creada correctamente",
  "recursos": ["notificaciones.email.queue", "notificaciones.email.queue.dlx", "notificaciones.email.queue.dlq"] }
```

**Validaciones** (DTO con Bean Validation y `GlobalExceptionHandler`):

- El nombre es obligatorio, tiene de 3 a 100 caracteres (letras, números, `.`, `_` o `-`) y no puede empezar con `amq.`, un prefijo que RabbitMQ reserva para sus recursos.
- `ttlMs` y `maxMensajes` deben ser positivos y con tope.
- Una cola `QUORUM` debe ser durable.
- El tipo de exchange solo acepta valores válidos; si no, la respuesta indica cuáles son.
- Un binding solo se crea si la cola y el exchange existen. Una cola o un exchange que ya existe responde 409.
- Todos los errores usan el mismo formato: `{ timestamp, status, error, detalles[], ruta }`.

`RabbitAdminService` es la única clase que usa `RabbitAdmin`; el controlador solo llama a sus métodos.

---

## 4b. Kafka (EP3)

### Tópicos y consumer groups

| Tópico | Key | Particiones / RF | Retención · cleanup | Productor | Consumer group (3 hilos) |
|---|---|---|---|---|---|
| `pedidos360.ventas.confirmadas` | `usuarioId` | 3 / 3, `min.insync.replicas=2` | 7 días · `delete` | `venta-carrito` al confirmar un pedido | `catalogo-metricas-ventas` → ranking de más vendidos |
| `pedidos360.inventario.actualizado` | `productoId` | 3 / 3, `min.insync.replicas=2` | `compact` (último estado por producto) | `productos-catalogo` al crear, editar, eliminar o descontar stock | `carrito-sync-inventario` → precio y disponibilidad en los carritos |
| `<tópico>.DLT` | igual | 1 / 3 | 14 días · `delete` | `DeadLetterPublishingRecoverer` | — (se inspecciona con Kafka-UI) |

Los tópicos se crean de forma explícita (`auto.create.topics=false`): con el contenedor `kafka-init` (`docker/kafka/crear-topicos.sh`), con `NewTopic` en cada microservicio o con `kafka-admin`. Los nombres, grupos y reintentos están solo en el bloque `pedidos360.kafka` de cada `application.yml`.

### Cliente, productores y consumidores

- **Cliente** (`spring.kafka`): bootstrap con los 3 brokers, `StringSerializer` / `JsonSerializer`, `acks=all`, idempotencia, `ErrorHandlingDeserializer`, `CooperativeStickyAssignor` y concurrencia 3.
- **Productores** (`messaging/kafka/**/Kafka*EventPublisher`): publican DTO tipados (`VentaConfirmadaEvent`, `InventarioActualizadoEvent`) **después del commit** de la transacción, con la key que garantiza el orden por cliente (`usuarioId`) o por producto (`productoId`).
- **Consumidores** (`@KafkaListener`): solo validan y delegan; la lógica está en servicios sin dependencias de Kafka (`MetricasVentaService` con idempotencia por `eventoId`, `SincronizacionInventarioService` que descarta eventos atrasados).
- **Errores**: `DefaultErrorHandler` con backoff exponencial (1 s, 2 s, 4 s) y luego DLT. Los mensajes inválidos (JSON roto, campos faltantes, key distinta del id) van directo a la DLT sin reintentos, para no bloquear la partición. Cada caso queda en el log con las etiquetas `KAFKA-CONSUMIDOR`, `KAFKA-REINTENTO` y `KAFKA-DLT`.

### API del administrador de Kafka (`kafka-admin`, puerto 8084)

Swagger: http://localhost:8084/swagger-ui.html · Todas las rutas exigen JWT.

| Método | Ruta | Qué hace |
|---|---|---|
| GET | `/api/kafka/cluster` | Brokers, controlador e id del clúster |
| GET | `/api/kafka/topicos` | Lista los tópicos (`?incluirInternos=true` para ver los internos) |
| POST | `/api/kafka/topicos` | Crea un tópico con particiones, factor de replicación y configuraciones |
| GET | `/api/kafka/topicos/{nombre}` | Detalle: particiones, líderes, réplicas e ISR |
| PATCH | `/api/kafka/topicos/{nombre}/particiones` | Aumenta particiones (Kafka no permite reducirlas → 400) |
| DELETE | `/api/kafka/topicos/{nombre}` | Elimina un tópico (los internos están protegidos → 400) |
| GET | `/api/kafka/topicos/{nombre}/configuraciones` | Configuraciones efectivas (`retention.ms`, `cleanup.policy`, etc.) |
| PUT | `/api/kafka/topicos/{nombre}/configuraciones` | Modifica configuraciones |
| DELETE | `/api/kafka/topicos/{nombre}/configuraciones/{clave}` | Restablece una configuración a su valor por defecto |
| GET | `/api/kafka/grupos` | Lista los consumer groups y su estado |
| GET | `/api/kafka/grupos/{groupId}` | Miembros, offsets y lag por partición |
| POST | `/api/kafka/grupos/{groupId}/reset-offsets` | Mueve los offsets a `EARLIEST` o `LATEST` (grupo sin consumidores activos) |
| DELETE | `/api/kafka/grupos/{groupId}` | Elimina un grupo inactivo |

La colección de Postman trae una carpeta con todas estas peticiones.

---

## 5. Seguridad

Todos los endpoints de los 4 microservicios exigen un JWT válido de Azure AD; `SecurityConfig` valida el emisor (tenant) y la audiencia (client id). Las únicas rutas públicas son `/ping` y la documentación Swagger. Los tests `SeguridadJwtTest` comprueban que una petición sin token recibe 401.

Solo para pruebas locales del administrador sin token: `SEGURIDAD_JWT_HABILITADO=false`.

---

## 6. Cómo ejecutar

1. **Levantar la infraestructura** desde la raíz del repositorio: `docker compose up -d` levanta el clúster RabbitMQ (3 nodos), Kafka (Zookeeper + 3 brokers + creación de tópicos), Kafka-UI (http://localhost:8090) y MySQL. Las bases `catalogo_db` y `carrito_db` se crean solas.
2. **Compilar y correr los tests** de los cuatro microservicios, desde esta carpeta:
   ```bash
   mvn clean test
   ```
3. **Levantar cada microservicio** (una terminal por cada uno):
   ```bash
   cd productos-catalogo && mvn spring-boot:run
   cd venta-carrito      && mvn spring-boot:run
   cd rabbitmq-admin     && mvn spring-boot:run
   cd kafka-admin        && mvn spring-boot:run
   ```
   Al arrancar, `productos-catalogo` y `venta-carrito` crean solos sus exchanges, colas, DLQ y bindings (http://localhost:15672, admin / admin123) y se unen a sus consumer groups de Kafka (Kafka-UI → *Consumers*).

> **Si al arrancar aparece `PRECONDITION_FAILED - inequivalent arg`:** en el broker ya existe una cola o un exchange con ese nombre pero con otra configuración (por ejemplo, uno creado a mano en versiones anteriores). Hay que eliminarlo desde el dashboard o con `DELETE` del administrador y reiniciar el microservicio. Lo mismo aplica si se cambian el TTL o el largo máximo en el YAML: RabbitMQ no permite modificar los argumentos de una cola que ya existe.

### Variables de entorno

| Variable | Por defecto | Uso |
|---|---|---|
| `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` | localhost:3306, root/root | Base de datos (RDS en la nube) |
| `RABBITMQ_ADDRESSES` | `localhost:5672,localhost:5673,localhost:5674` | Nodos del clúster RabbitMQ (catálogo y carrito) |
| `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USER`, `RABBITMQ_PASSWORD` | localhost:5672, admin/admin123 | Broker (`rabbitmq-admin`) y credenciales |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092,localhost:9093,localhost:9094` | Brokers Kafka (en Docker: `kafka1:29092,kafka2:29092,kafka3:29092`) |
| `KAFKA_REPLICAS`, `KAFKA_MIN_ISR` | 3, 2 | Factor de replicación y `min.insync.replicas` de los tópicos que declara cada microservicio |
| `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` | App Registration del proyecto | Validación del JWT |
| `CORS_ORIGINS` | `http://localhost:4200,http://52.200.216.83:4200` | URL del frontend (cambia con la IP de la EC2) |
| `DLQ_AUDITOR_ACTIVO` | `true` | `false` deja los mensajes acumulados en la DLQ para verlos en el dashboard |
| `SEGURIDAD_JWT_HABILITADO` | `true` | `productos-catalogo` y `rabbitmq-admin`: `false` desactiva el JWT solo para pruebas locales sin token |

En PowerShell: `$env:DLQ_AUDITOR_ACTIVO="false"; mvn spring-boot:run`

---

## 7. Pruebas y evidencias

La colección `postman/Pedidos360-backend.postman_collection.json` trae todas las peticiones. Basta con pegar el JWT en la variable `token`.

### Pruebas automatizadas (`mvn clean test`)

**93 tests en verde** (catálogo 35 · carrito 32 · rabbitmq-admin 15 · kafka-admin 11) y 8 omitidos: los de `KafkaAdminServiceClusterLocalTest`, que necesitan el clúster levantado y se ejecutan con `-Dkafka.local=true`.

| Microservicio | Qué se prueba |
|---|---|
| productos-catalogo | ACK, NACK a la DLQ y reintentos del consumidor (`StockPedidoConsumerTest`) · descuento de stock (`ProductoServiceTest`) · beans de RabbitMQ con DLX/TTL (`RabbitMQConfigTest`) · 401 sin JWT (`SeguridadJwtTest`) · CRUD (`ProductoControllerTest`) · **Kafka:** métricas de venta, idempotencia y validación de coherencia (`MetricasVentaServiceTest`), consumidor que valida y delega (`VentasConfirmadasConsumerTest`), productor con key = productoId, envío tras el commit, tombstone y tolerancia a Kafka caído (`KafkaInventarioEventPublisherTest`), mensajes inválidos al DLT sin reintentos y errores transitorios con reintentos (`KafkaErrorHandlerTest`) |
| venta-carrito | Confirmación de pedido y carrito vacío (`CarritoServiceTest`, `CarritoControllerTest`) · publicación en exchange y routing key correctos (`RabbitPedidoEventPublisherTest`) · consumidor de tickets (`TicketConsumerTest`) · formato del ticket (`TicketServiceTest`) · **Kafka:** disponibilidad y precio en los carritos, descarte de eventos atrasados y tombstones (`SincronizacionInventarioServiceTest`), validación de la key y del payload (`InventarioActualizadoConsumerTest`), DTO armado desde el pedido, key = usuarioId y envío tras el commit (`KafkaVentaEventPublisherTest`), clasificación de errores (`KafkaErrorHandlerTest`) |
| rabbitmq-admin | Rutas y códigos HTTP, y validaciones 400, 404 y 409 (`RabbitAdminControllerTest`, `RabbitAdminServiceTest`) |
| kafka-admin | Rutas, códigos HTTP y validaciones (`KafkaAdminControllerTest`) · 401 sin JWT (`SeguridadJwtTest`) · contra el clúster real, con `-Dkafka.local=true`: crear, modificar, aumentar particiones, grupos, reset de offsets y eliminar (`KafkaAdminServiceClusterLocalTest`) |

Las pruebas de punta a punta de Kafka (idempotencia, reintentos, DLT, consumer groups) están documentadas en [docs/kafka/casos-de-uso-kafka.md §10](../docs/kafka/casos-de-uso-kafka.md).

### Pruebas manuales con el dashboard de RabbitMQ

Se puede publicar un evento a mano sin pasar por el frontend: **Exchanges → `pedidos.exchange` → Publish message**, con routing key `pedido.creado`, la propiedad `content_type = application/json` y este payload:

```json
{"pedidoId":"DEMO-1","usuarioId":"demo","total":9980,
 "items":[{"productoId":1,"nombreProducto":"Hamburguesa","cantidad":2,"precioUnitario":4990}]}
```

| # | Escenario | Cómo provocarlo | Qué se ve en los logs y el dashboard |
|---|---|---|---|
| 1 | Flujo completo | Confirmar un carrito desde el frontend o Postman, o publicar el JSON de arriba | `[PRODUCTOR] ... publicado` · `[STOCK] OK ... ACK` · `[TICKET] Comprobante generado` · el stock del producto baja |
| 2 | Regla de negocio | Publicar con `"productoId":999` o `"cantidad":9999` | `[STOCK] Error NO recuperable ... -> DLQ` · `[DLQ] ... motivo: rejected` |
| 3 | Mensaje inválido | Publicar `hola` (no es JSON) | NACK inmediato → `[DLQ] ... motivo: rejected` en ambas DLQ |
| 4 | Error recuperable | `docker stop mysql-server` y publicar el JSON | `[STOCK] Error recuperable en intento 1/3 ... 2/3` → DLQ. Con `docker start mysql-server` a tiempo, termina en ACK |
| 5 | Retención (TTL) | Detener `productos-catalogo`, publicar y esperar 60 s | El mensaje pasa a `pedidos.stock.dlq` con motivo `expired` |
| 6 | Ver la DLQ con mensajes | Levantar con `DLQ_AUDITOR_ACTIVO=false` y repetir el escenario 2 | **Queues → `pedidos.stock.dlq` → Get messages** muestra el mensaje y su header `x-death` |
| 7 | Administrador | Swagger o Postman: crear cola con DLQ, exchange y binding; luego eliminarlos | 201 / 204, y los recursos aparecen y desaparecen del dashboard |
| 8 | Validaciones | Crear una cola con `"nombre": ""`, `"amq.x"` o `QUORUM` con `durable:false` | 400 con el detalle de cada error |

---

## 8. Integración con frontend e infraestructura

- **Frontend:** nuevo endpoint `POST /api/carrito/{usuarioId}/confirmar` (con JWT). Responde 202 y el JSON del pedido. Si el carrito está vacío responde 422, y si RabbitMQ no está disponible, 503.
- **Infraestructura:** hay que desplegar el nuevo microservicio `rabbitmq-admin` en el puerto 8083 (ya está dentro del rango 8080–8090 del security group). En cada contenedor se pasa `RABBITMQ_HOST` y `CORS_ORIGINS`.
- **Endpoints retirados:** `/api/admin/broker/**` (antes en catálogo), reemplazado por `rabbitmq-admin`, y `/api/carrito/test-mensaje`, reemplazado por la confirmación real. La cola antigua `pedidos.cola` se puede eliminar.
