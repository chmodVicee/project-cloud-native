# Mensajería asíncrona en Pedidos360 (RabbitMQ + GCP Pub/Sub)

Documento exclusivo de la mensajería: arquitectura, configuración, cómo se usa dentro de
la aplicación, cómo levantarla y cómo probarla. La visión general del sistema está en el
[README principal](README.md).

## Índice

1. [Resumen](#1-resumen)
2. [Arquitectura](#2-arquitectura)
3. [Configuración](#3-configuración)
4. [Cómo se usa RabbitMQ en la aplicación](#4-cómo-se-usa-rabbitmq-en-la-aplicación)
5. [Cómo levantarlo](#5-cómo-levantarlo)
6. [Cómo probarlo](#6-cómo-probarlo)

---

## 1. Resumen

Cuando un pedido se **crea** o **cambia de estado**, `ms-pedidos360-orders` guarda el
cambio en su base de datos y, **después del commit**, publica un evento en RabbitMQ. Las
tareas que no tienen que hacer esperar al usuario se ejecutan de forma asíncrona en
consumidores independientes:

| Pieza | Rol |
|-------|-----|
| **RabbitMQ** | Mensajería **interna** entre componentes: exchanges, colas, bindings, reintentos y DLQ (cola de mensajes muertos). Es lo que usan los productores y consumidores de los microservicios. |
| **GCP Pub/Sub** | Cola de mensajes **en la nube**: cada estado se reenvía al topic `pedidos360-order-status` para que otros sistemas en GCP (analítica, notificaciones externas, auditoría) lo consuman. En local se usa el emulador oficial, sin credenciales. |
| **ms-pedidos360-rabbit-admin** | Microservicio que expone una API REST para crear y eliminar colas, exchanges y bindings. |

El estado del pedido **solo cambia cuando un ADMIN lo modifica** desde `/admin/ordenes`
(`PUT /api/orders/{id}/status`). La simulación automática anterior fue eliminada. El
frontend no necesitó cambios: el `PUT` responde igual que antes y las pantallas de pedidos
se refrescan cada 4 s.

Si RabbitMQ o Pub/Sub fallan, **la lógica existente no se ve afectada**: el pedido queda
guardado y el problema se registra en los logs (y, del lado de los consumidores, el
mensaje se reintenta y termina en la DLQ).

---

## 2. Arquitectura

```
                         ms-pedidos360-orders
  ┌───────────────────────────────────────────────────────────────────────────────┐
  │ OrderService ──(OrderStatusEvent de Spring)──► OrderEventProducer             │
  │   crea / cambia estado          después del commit │ publisher confirms        │
  └────────────────────────────────────────────────────┼──────────────────────────┘
                                                       ▼
                          ┌───────────────────────────────────────────┐
                          │ pedidos360.orders.exchange   (topic)      │
                          │  routing keys: order.created              │
                          │                order.status.changed       │
                          └───────┬──────────────────────────┬────────┘
                         order.#  │                          │  order.#
                                  ▼                          ▼
                orders.notifications.queue        orders.cloud-bridge.queue
                          │                                  │
                          ▼                                  ▼
             OrderNotificationConsumer           OrderCloudBridgeConsumer ──► GCP Pub/Sub
             ("[NOTIFICACIÓN]" al cliente)                                  topic pedidos360-order-status
                          │                                  │                       │
       error transitorio  │                                  │                       ▼
   ┌──────────────────────┴──────────────┐                   │      suscripción pedidos360-order-status-audit
   ▼                                     │ error definitivo  │                       │
 pedidos360.orders.retry (direct)        │ o sin intentos    │                       ▼
   │ routing key = nombre de la cola     ▼                   ▼          PubSubOrderStatusSubscriber ("[PUBSUB]")
   ▼                              pedidos360.orders.dlx (direct, DLX)
 orders.<caso>.retry.queue           │ routing key = nombre de la DLQ
   │ TTL 5 s, sin consumidores       ▼
   └──► vuelve a la cola principal   orders.notifications.dlq / orders.cloud-bridge.dlq
        (exchange por defecto)          │
                                        ▼
                                  DeadLetterConsumer (log del mensaje no entregado + ACK)
```

### Exchanges, colas y bindings

| Tipo | Nombre | Detalle |
|------|--------|---------|
| Exchange `topic` | `pedidos360.orders.exchange` | Eventos de pedidos |
| Exchange `direct` | `pedidos360.orders.retry` | Lleva los mensajes a su cola de espera |
| Exchange `direct` (DLX) | `pedidos360.orders.dlx` | Lleva los mensajes rechazados a su DLQ |
| Cola | `orders.notifications.queue` | `x-dead-letter-exchange=pedidos360.orders.dlx`, `x-dead-letter-routing-key=orders.notifications.dlq` |
| Cola | `orders.notifications.retry.queue` | `x-message-ttl=5000`, DLX `""` → `orders.notifications.queue` |
| Cola | `orders.notifications.dlq` | DLQ de notificaciones |
| Cola | `orders.cloud-bridge.queue` | Igual que la de notificaciones, con su DLQ `orders.cloud-bridge.dlq` |
| Cola | `orders.cloud-bridge.retry.queue` | `x-message-ttl=5000`, DLX `""` → `orders.cloud-bridge.queue` |
| Cola | `orders.cloud-bridge.dlq` | DLQ del puente a Pub/Sub |
| Binding | exchange `orders` → `notifications.queue` | `order.#` |
| Binding | exchange `orders` → `cloud-bridge.queue` | `order.#` |
| Binding | exchange `retry` → `*.retry.queue` | nombre de la cola principal |
| Binding | exchange `dlx` → `*.dlq` | nombre de la DLQ |

Todo se declara como **beans** en `config/RabbitMQConfig`. Spring AMQP crea en el broker
lo que falte al arrancar `orders`.

### Mensaje (`OrderStatusEvent`, JSON)

```json
{
  "eventId": "7a6f22d1-5dc1-4e42-8945-1a965b473861",
  "orderId": 3,
  "userEmail": "cliente@test.cl",
  "previousStatus": "RECIBIDO",
  "newStatus": "PREPARANDO",
  "total": 2490.00,
  "occurredAt": "2026-10-28T12:49:05.123-03:00"
}
```

`previousStatus` es `null` en `order.created`. `eventId` también viaja como `message_id`
de AMQP. Son obligatorios `orderId`, `userEmail` y `newStatus`.

### Estructura del código (`ms-pedidos360-orders`)

```
com.pedidos360.orders
├── config/
│   ├── MessagingProperties      ← nombres de exchanges, colas, routing keys y reintentos (messaging.rabbitmq.*)
│   ├── RabbitMQConfig           ← beans Exchange/Queue/Binding, RabbitTemplate, fábrica de listeners
│   ├── PubSubProperties         ← configuración de Pub/Sub (messaging.pubsub.*)
│   └── PubSubConfig             ← canal/credenciales, Publisher, creación de topic y suscripción
├── event/OrderStatusEvent       ← evento de dominio = contenido del mensaje
├── service/
│   ├── OrderService             ← lógica de negocio; solo emite OrderStatusEvent (no conoce RabbitMQ)
│   └── OrderNotificationService ← tarea asíncrona: notificar al cliente
└── messaging/
    ├── producer/
    │   ├── OrderEventProducer           ← publica en RabbitMQ después del commit
    │   └── cloud/                       ← CloudStatusPublisher (interfaz), PubSubStatusPublisher, DisabledCloudStatusPublisher
    ├── consumer/
    │   ├── notification/OrderNotificationConsumer
    │   ├── cloud/OrderCloudBridgeConsumer, PubSubOrderStatusSubscriber
    │   └── deadletter/DeadLetterConsumer
    └── support/                         ← OrderEventReader, MessageRetryHandler, excepciones recuperable/no recuperable
```

---

## 3. Configuración

### 3.1 Variables de entorno

Se definen en `.env` (copia de `.env.example`). `docker-compose` las pasa a los
contenedores; en ejecución local cada microservicio las lee de su propio `.env`
(copia de su `.env-example`).

| Variable | Usada por | Valor por defecto | Descripción |
|----------|-----------|-------------------|-------------|
| `RABBITMQ_USER` | rabbitmq, orders, rabbit-admin | `pedidos360` | Usuario del broker |
| `RABBITMQ_PASSWORD` | rabbitmq, orders, rabbit-admin | `pedidos360` en compose | Clave del broker (en `.env.example` es un valor de ejemplo, cámbiala) |
| `RABBITMQ_HOST` / `RABBITMQ_PORT` | orders, rabbit-admin | `localhost` / `5672` | En compose: `rabbitmq` / `5672` |
| `MESSAGING_RETRY_MAX_ATTEMPTS` | orders | `3` | Intentos totales antes de la DLQ |
| `MESSAGING_RETRY_DELAY_MS` | orders | `5000` | Espera entre reintentos (TTL de la cola de espera) |
| `GCP_PUBSUB_ENABLED` | orders | `false` (compose: `true`) | Activa el reenvío a Pub/Sub y el subscriber |
| `GCP_PROJECT_ID` | orders, pubsub-emulator | `pedidos360-local` | Proyecto de GCP (ficticio en el emulador) |
| `PUBSUB_EMULATOR_HOST` | orders | vacío (compose: `pubsub-emulator:8085`) | Si tiene valor, usa el emulador; vacío = GCP real con ADC |
| `GCP_PUBSUB_CREATE_RESOURCES` | orders | `true` | Crea topic y suscripción al arrancar |
| `RABBIT_ADMIN_URL` | bff | `http://localhost:8084` | Dirección del administrador para el ruteo `/api/rabbit/**` |

> No subas credenciales reales: `.env` está en `.gitignore`. Para GCP real usa
> `GOOGLE_APPLICATION_CREDENTIALS` apuntando a un archivo **fuera** del repositorio.

### 3.2 Conexión y comportamiento del broker (`spring.rabbitmq.*` en orders)

```yaml
spring:
  rabbitmq:
    host: ${RABBITMQ_HOST:localhost}
    port: ${RABBITMQ_PORT:5672}
    username: ${RABBITMQ_USER:pedidos360}
    password: ${RABBITMQ_PASSWORD}
    publisher-confirm-type: correlated   # el broker confirma cada publicación
    publisher-returns: true              # devuelve los mensajes que no llegan a ninguna cola
    listener:
      simple:
        acknowledge-mode: manual         # cada consumidor hace basicAck / basicNack
        prefetch: 10                     # máximo de mensajes sin confirmar por consumidor
        default-requeue-rejected: false  # un rechazo nunca vuelve a la cola: va a la DLQ
```

### 3.3 Nombres centralizados (`messaging.rabbitmq.*`)

Ninguna clase escribe nombres de colas o exchanges a mano. Todos salen de
`application.yaml` y se leen con el record `MessagingProperties`
(`@ConfigurationProperties` validado con `@NotBlank`/`@Min`):

```yaml
messaging:
  rabbitmq:
    exchanges:
      orders: pedidos360.orders.exchange       # → MessagingProperties.exchanges().orders()
      retry: pedidos360.orders.retry
      dead-letter: pedidos360.orders.dlx
    routing-keys:
      order-created: order.created             # usada por OrderEventProducer
      order-status-changed: order.status.changed
      all-order-events: "order.#"              # usada en los bindings de las colas
    queues:
      notifications:                           # un bloque por caso de uso
        name: orders.notifications.queue
        retry-queue: orders.notifications.retry.queue
        dead-letter-queue: orders.notifications.dlq
      cloud-bridge:
        name: orders.cloud-bridge.queue
        retry-queue: orders.cloud-bridge.retry.queue
        dead-letter-queue: orders.cloud-bridge.dlq
    retry:
      max-attempts: ${MESSAGING_RETRY_MAX_ATTEMPTS:3}
      delay-ms: ${MESSAGING_RETRY_DELAY_MS:5000}
```

| Propiedad | Clase que la usa | Para qué |
|-----------|------------------|----------|
| `exchanges.*` | `RabbitMQConfig`, `OrderEventProducer`, `MessageRetryHandler` | Declarar exchanges y publicar/reintentar |
| `routing-keys.order-*` | `OrderEventProducer` | Elegir la routing key según el evento |
| `routing-keys.all-order-events` | `RabbitMQConfig` | Binding de cada cola al exchange topic |
| `queues.<caso>.*` | `RabbitMQConfig`, consumidores (`@RabbitListener(queues = "${...}")`) | Declarar colas y escucharlas |
| `retry.*` | `RabbitMQConfig` (TTL), `MessageRetryHandler` | Cantidad de intentos y espera |

Para **renombrar** una cola o un exchange basta con cambiar el YAML (o sobreescribir la
propiedad con una variable de entorno, por ejemplo
`MESSAGING_RABBITMQ_QUEUES_NOTIFICATIONS_NAME=otra.cola`). No hay que tocar código.

> Si cambias argumentos de una cola que ya existe (TTL, DLX), RabbitMQ rechaza la
> redeclaración (`PRECONDITION_FAILED`). Elimínala antes desde la consola o reinicia el
> volumen con `docker-compose down -v`.

### 3.4 Pub/Sub (`messaging.pubsub.*`)

```yaml
messaging:
  pubsub:
    enabled: ${GCP_PUBSUB_ENABLED:false}
    project-id: ${GCP_PROJECT_ID:pedidos360-local}
    emulator-host: ${PUBSUB_EMULATOR_HOST:}
    topic: pedidos360-order-status
    subscription: pedidos360-order-status-audit
    publish-timeout: 10s                       # si Pub/Sub no confirma a tiempo → reintento
    create-resources: ${GCP_PUBSUB_CREATE_RESOURCES:true}
```

`PubSubConfig` solo se activa con `enabled: true`. Con `emulator-host` usa un canal gRPC
en texto plano y sin credenciales; sin él usa las credenciales por defecto de Google
(ADC). Con `enabled: false` se activa `DisabledCloudStatusPublisher`, que solo registra el
evento, y el mensaje de RabbitMQ se confirma normalmente.

### 3.5 Administrador (`rabbit-admin.*`)

```yaml
rabbit-admin:
  protected-prefixes: [amq., pedidos360., orders.]   # no se pueden crear ni eliminar por la API
```

---

## 4. Cómo se usa RabbitMQ en la aplicación

### 4.1 Productor

1. `OrderService.create(...)` y `OrderService.updateStatus(...)` guardan el pedido y
   llaman a `ApplicationEventPublisher.publishEvent(OrderStatusEvent.of(order, previo))`.
   `OrderService` **no conoce RabbitMQ**. Si el estado no cambia, no se emite evento.
2. `OrderEventProducer.publish(...)` está anotado con
   `@TransactionalEventListener(phase = AFTER_COMMIT)`: solo se ejecuta si la transacción
   se confirmó. Si la transacción falla, no sale ningún mensaje.
3. Elige la routing key (`order.created` si `previousStatus == null`, si no
   `order.status.changed`) y envía con `RabbitTemplate.convertAndSend` (JSON, con
   `message_id` = `eventId`).
4. El `RabbitTemplate` (`RabbitMQConfig`) tiene `mandatory=true`, *publisher confirms* y
   *returns*. Un NACK del broker o un mensaje no enrutable quedan en el log. Si el broker
   está caído, el productor registra el error y **la petición HTTP responde igual**,
   porque el pedido ya quedó guardado.

### 4.2 Consumidores

Todos usan `@RabbitListener`, ACK manual y están agrupados por dominio funcional en
`messaging/consumer/<dominio>`:

| Consumidor | Escucha | Hace |
|------------|---------|------|
| `notification/OrderNotificationConsumer` | `orders.notifications.queue` | Verifica que el pedido exista y registra `[NOTIFICACIÓN] Para <email>: ...` |
| `cloud/OrderCloudBridgeConsumer` | `orders.cloud-bridge.queue` | Reenvía el JSON a Pub/Sub y espera su confirmación |
| `deadletter/DeadLetterConsumer` | ambas DLQ | Registra `[DLQ ...]` con la cola de origen, el motivo (`x-death`) y el contenido, y hace ACK |
| `cloud/PubSubOrderStatusSubscriber` | suscripción de Pub/Sub | Registra `[PUBSUB] ...`; ACK si tiene datos y NACK si no |

Los listeners reciben el `Message` **sin convertir**: la fábrica
`rabbitListenerContainerFactory` usa `SimpleMessageConverter`, y `OrderEventReader`
deserializa explícitamente. Así un JSON inválido también llega al consumidor, que lo
envía a la DLQ, en vez de fallar dentro del contenedor de Spring.

### 4.3 Confirmación y manejo de errores

Cada consumidor sigue el mismo esquema:

```java
try {
    procesar(eventReader.read(message));                 // lógica del caso de uso
    channel.basicAck(deliveryTag, false);                 // éxito
} catch (NonRecoverableMessageException ex) {             // (+ ResourceNotFoundException en notificaciones)
    retryHandler.reject(message, channel, queue, ex);     // NACK sin requeue → DLQ
} catch (RuntimeException ex) {
    retryHandler.retryOrReject(message, channel, queue, ex); // reintento o DLQ
}
```

| Situación | Tipo | Respuesta | Resultado |
|-----------|------|-----------|-----------|
| Procesado correctamente | - | `basicAck` | Sale de la cola |
| JSON inválido o sin `orderId`/`userEmail`/`newStatus` | No recuperable | `basicNack(requeue=false)` | DLX → DLQ |
| Pedido inexistente (notificaciones) | No recuperable | `basicNack(requeue=false)` | DLX → DLQ |
| Base de datos caída, Pub/Sub sin respuesta, error inesperado | Recuperable | Copia a `pedidos360.orders.retry` con `x-retry-count+1` y `basicAck` del original | Vuelve tras 5 s |
| Error recuperable en el 3er intento | Recuperable agotado | `basicNack(requeue=false)` | DLX → DLQ |
| No se pudo programar el reintento | - | `basicNack(requeue=false)` | DLX → DLQ |

* **Nunca** se hace `NACK` con *requeue*: un mensaje con errores no puede quedar en un
  bucle infinito.
* El reintento usa una **cola de espera con TTL** sin consumidores. Al expirar, RabbitMQ
  la devuelve a la cola principal por el exchange por defecto. Así se espera sin
  bloquear el hilo del consumidor.
* Cada decisión deja un log: `WARN` en los reintentos, `ERROR` en el envío a DLQ y
  `ERROR [DLQ ...]` cuando el `DeadLetterConsumer` lo procesa.

### 4.4 GCP Pub/Sub

* `OrderCloudBridgeConsumer` → `CloudStatusPublisher` (interfaz) →
  `PubSubStatusPublisher`, que publica `data` = JSON del evento y los atributos
  `orderId`, `newStatus` y `eventId`, y espera el `messageId` hasta `publish-timeout`.
  Si no llega, lanza `RecoverableMessageException` → reintento en RabbitMQ → DLQ.
* `PubSubOrderStatusSubscriber` (`SmartLifecycle`) escucha la suscripción
  `pedidos360-order-status-audit` y registra cada estado recibido.
* El topic y la suscripción se crean al arrancar si no existen. Si Pub/Sub no responde
  en ese momento, la aplicación arranca igual.

### 4.5 Microservicio administrador (`ms-pedidos360-rabbit-admin`)

`RabbitAdminService` concentra todas las operaciones sobre `AmqpAdmin`. Los
controladores (`QueueController`, `ExchangeController`, `BindingController`) solo
validan y delegan. Vía BFF todas las rutas requieren rol **ADMIN**.

| Método y ruta | Cuerpo / parámetros | Respuestas |
|---------------|---------------------|------------|
| `POST /api/rabbit/queues` | `{"name", "durable"?, "autoDelete"?, "deadLetterExchange"?, "deadLetterRoutingKey"?, "messageTtlMs"?, "maxLength"?}` | `201` cola creada · `400` datos inválidos · `404` DLX inexistente · `409` ya existe o protegida |
| `GET /api/rabbit/queues/{name}` | - | `200 {"name","messageCount","consumerCount"}` · `404` |
| `DELETE /api/rabbit/queues/{name}` | `?ifUnused=true&ifEmpty=true` opcionales | `204` · `404` · `409` con consumidores/mensajes o protegida |
| `POST /api/rabbit/exchanges` | `{"name", "type": DIRECT\|TOPIC\|FANOUT\|HEADERS, "durable"?}` | `201` · `400` · `409` |
| `DELETE /api/rabbit/exchanges/{name}` | - | `204` · `404` · `409` |
| `POST /api/rabbit/bindings` | `{"exchange", "destination", "destinationType": QUEUE\|EXCHANGE, "routingKey"?}` | `201` · `400` · `404` si no existe el exchange o el destino |
| `DELETE /api/rabbit/bindings` | `?exchange=&destination=&destinationType=&routingKey=` | `204` · `400` · `404` |

Validaciones (`dto/`, Bean Validation): nombres obligatorios de 1 a 255 caracteres con
`[A-Za-z0-9._:-]`; routing keys con `*` y `#`; `type` y `destinationType` dentro del
enum; `messageTtlMs` y `maxLength` positivos; `deadLetterRoutingKey` requiere
`deadLetterExchange`. Los errores se devuelven por campo:

```json
{ "name": "debe tener entre 1 y 255 caracteres y solo letras, números, '.', '_', ':' o '-'" }
```

### 4.6 Agregar un nuevo caso de uso (ejemplo: "generar ticket")

1. **YAML**: agrega `messaging.rabbitmq.queues.tickets.{name,retry-queue,dead-letter-queue}`.
2. **Propiedades**: agrega `QueueRoute tickets` al record `MessagingProperties.Queues`.
3. **Beans**: en `RabbitMQConfig` declara `ticketsQueue` (con `mainQueue(...)`),
   `ticketsRetryQueue` (con `retryQueue(...)`), la DLQ y sus tres bindings (exchange
   principal, retry y DLX).
4. **Consumidor**: crea `messaging/consumer/ticket/OrderTicketConsumer` con
   `@RabbitListener(queues = "${messaging.rabbitmq.queues.tickets.name}")`, siguiendo
   el esquema ACK/NACK de la sección 4.3, y delega la lógica en un servicio.
5. **DLQ**: agrega la nueva DLQ a `queues` en `DeadLetterConsumer`.
6. **Test**: copia `OrderNotificationConsumerTest` y adapta los casos.

---

## 5. Cómo levantarlo

### Con Docker (recomendado)

```bash
cp .env.example .env           # completa los valores (Azure, RabbitMQ, etc.)
docker-compose up --build
```

| Servicio | URL |
|----------|-----|
| Consola de RabbitMQ | http://localhost:15672 (usuario/clave = `RABBITMQ_USER` / `RABBITMQ_PASSWORD`) |
| AMQP | `localhost:5672` |
| Emulador de Pub/Sub | `localhost:8085` |
| Administrador (depuración, sin JWT) | http://localhost:8084/api/rabbit/... |
| Orders (depuración, sin JWT) | http://localhost:8082/api/orders |
| BFF (con JWT) | http://localhost:8080 |

Para ver solo los logs de mensajería:

```bash
docker logs -f ms-pedidos360-orders | grep -E "Evento publicado|NOTIFICACIÓN|PUBSUB|DLQ|reintenta"
```

### Solo la infraestructura (para ejecutar los servicios con Maven)

```bash
docker-compose up -d rabbitmq pubsub-emulator
cd ms-pedidos360-orders && cp .env-example .env && ./mvnw spring-boot:run
cd ms-pedidos360-rabbit-admin && cp .env-example .env && ./mvnw spring-boot:run
```

En local `PUBSUB_EMULATOR_HOST=localhost:8085`.

---

## 6. Cómo probarlo

Los ejemplos van directo a los puertos de depuración (`8082`, `8084`). Para ir por el
BFF (`8080`) agrega `-H "Authorization: Bearer $TOKEN"` y cambia el puerto. El token se
obtiene con `POST /api/auth/login` o desde `localStorage.pedidos360_token` en la SPA. Para
`/api/rabbit/**` y para cambiar estados por el BFF el token debe tener rol **ADMIN** (login
con Microsoft `@duocuc.cl`, o `UPDATE users SET role='ADMIN' WHERE username='...'` en
`users_db`).

### 6.1 Flujo normal: crear un pedido y cambiar su estado

```bash
# Crear pedido (publica order.created)
curl -X POST localhost:8082/api/orders -H "X-User-Email: cliente@test.cl" \
     -H "Content-Type: application/json" -d '{"items":[{"productId":1,"quantity":1}]}'

# Cambiar estado (publica order.status.changed)
curl -X PUT localhost:8082/api/orders/1/status -H "Content-Type: application/json" \
     -d '{"status":"PREPARANDO"}'
```

Logs esperados en `ms-pedidos360-orders`:

```
OrderEventProducer          : Evento publicado [order.status.changed] pedido #1: RECIBIDO -> PREPARANDO
OrderNotificationService    : [NOTIFICACIÓN] Para cliente@test.cl: tu pedido #1 pasó de RECIBIDO a PREPARANDO.
PubSubStatusPublisher       : Estado del pedido #1 (PREPARANDO) publicado en Pub/Sub pedidos360-order-status con id 2
PubSubOrderStatusSubscriber : [PUBSUB] estado recibido pedido #1 -> PREPARANDO (id 2): {...}
```

En el frontend, `/mis-pedidos` muestra el nuevo estado en el stepper en menos de 4 s.

### 6.2 Error no recuperable → DLQ

Publica un mensaje inválido desde la consola (**Exchanges → pedidos360.orders.exchange →
Publish message**, routing key `order.status.changed`, payload `esto no es json`) o
con la API HTTP de RabbitMQ:

```bash
curl -u "$RABBITMQ_USER:$RABBITMQ_PASSWORD" -X POST \
  localhost:15672/api/exchanges/%2F/pedidos360.orders.exchange/publish \
  -H "content-type: application/json" \
  -d '{"properties":{"message_id":"json-invalido"},"routing_key":"order.status.changed","payload":"esto no es json","payload_encoding":"string"}'
```

```
MessageRetryHandler : Mensaje json-invalido de orders.notifications.queue rechazado sin reintento; se envía a la DLQ. Causa: JSON inválido: ...
DeadLetterConsumer  : [DLQ orders.notifications.dlq] mensaje json-invalido no entregado. Cola de origen=orders.notifications.queue, motivo=rejected, contenido=esto no es json
```

Con un pedido inexistente (`{"eventId":"x","orderId":999999,"userEmail":"a@b.cl","newStatus":"LISTO"}`)
la notificación también va a la DLQ, mientras que el puente a la nube sí lo reenvía.

### 6.3 Error transitorio → reintentos → DLQ

```bash
docker stop pedidos360-pubsub        # Pub/Sub deja de responder
curl -X PUT localhost:8082/api/orders/1/status -H "Content-Type: application/json" -d '{"status":"LISTO"}'
# esperar ~1 min
docker start pedidos360-pubsub
```

```
OrderNotificationService : [NOTIFICACIÓN] Para cliente@test.cl: tu pedido #1 pasó de PREPARANDO a LISTO.   ← no se ve afectada
MessageRetryHandler      : Mensaje ... de orders.cloud-bridge.queue falló (intento 1/3); se reintenta en 5000 ms. Causa: Pub/Sub no confirmó ...
MessageRetryHandler      : Mensaje ... de orders.cloud-bridge.queue falló (intento 2/3); se reintenta en 5000 ms. ...
MessageRetryHandler      : Mensaje ... de orders.cloud-bridge.queue agotó 3 intentos; se envía a la DLQ. ...
DeadLetterConsumer       : [DLQ orders.cloud-bridge.dlq] mensaje ... no entregado. Cola de origen=orders.cloud-bridge.queue, motivo=rejected, ...
```

Si el servicio vuelve antes del tercer intento, el mensaje se procesa normalmente. Lo
mismo pasa con `docker stop pedidos360-db` en la cola de notificaciones.

### 6.4 Microservicio administrador

```bash
A=localhost:8084/api/rabbit
curl -X POST $A/exchanges -H "Content-Type: application/json" -d '{"name":"tickets.exchange","type":"TOPIC"}'      # 201
curl -X POST $A/exchanges -H "Content-Type: application/json" -d '{"name":"tickets.dlx","type":"DIRECT"}'          # 201
curl -X POST $A/queues -H "Content-Type: application/json" \
     -d '{"name":"tickets.queue","deadLetterExchange":"tickets.dlx","deadLetterRoutingKey":"tickets.dlq","messageTtlMs":60000}'  # 201
curl -X POST $A/bindings -H "Content-Type: application/json" \
     -d '{"exchange":"tickets.exchange","destination":"tickets.queue","destinationType":"QUEUE","routingKey":"ticket.#"}'     # 201
curl $A/queues/tickets.queue                                                                                         # 200
curl -X DELETE "$A/bindings?exchange=tickets.exchange&destination=tickets.queue&destinationType=QUEUE&routingKey=ticket.%23" # 204
curl -X DELETE $A/queues/tickets.queue                                                                               # 204
curl -X DELETE $A/exchanges/tickets.exchange                                                                         # 204

# Validaciones
curl -X POST $A/queues -H "Content-Type: application/json" -d '{"name":""}'                    # 400 {"name":"es obligatorio"}
curl -X POST $A/exchanges -H "Content-Type: application/json" -d '{"name":"x","type":"CIRCULAR"}'  # 400
curl -X DELETE $A/queues/orders.notifications.queue                                            # 409 prefijo protegido
curl -X DELETE $A/queues/no.existe                                                             # 404
```

Por el BFF: sin token `401`, con rol USER `403` y con rol ADMIN `2xx`/`4xx` según el caso.

### 6.5 Tests automatizados

```bash
cd ms-pedidos360-orders && ./mvnw test        # productor, consumidores y manejo de reintentos/DLQ (14 tests)
cd ms-pedidos360-rabbit-admin && ./mvnw test  # servicio administrador y validación de la API (16 tests)
```
