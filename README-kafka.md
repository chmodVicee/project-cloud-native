# Actividad Kafka 3.2.1 — productor, consumidor y scripts de carga

Microservicio `ms-pedidos360-kafka` de Pedidos360: un endpoint REST publica mensajes
en un topic Kafka y un `@KafkaListener` los consume mostrando partición, offset y key.
Incluye scripts para enviar muchos mensajes y comprobar el reparto por particiones.

> Esta guía es independiente de RabbitMQ / GCP Pub-Sub (`README-rabbitmq.md`): no
> modifica `orders` ni usa Kafka Streams (eso pertenece a la guía 3.2.2).

---

## Índice

- [Arquitectura](#arquitectura)
- [Requisitos](#requisitos)
- [Cómo levantar el entorno](#cómo-levantar-el-entorno)
- [Endpoints](#endpoints)
- [Scripts de carga](#scripts-de-carga)
- [Cómo ver los resultados](#cómo-ver-los-resultados)
- [Problemas comunes](#problemas-comunes)
- [Estructura del microservicio](#estructura-del-microservicio)

---

## Arquitectura

```
Cliente (curl / script)
        │  POST o GET /api/kafka/send?message=...&key=...
        ▼
KafkaController ──► KafkaMessageProducer ──► KafkaTemplate.send(topic, key, message)
                                                    │
                                                    ▼
                       broker Kafka (docker) ──► topic: tutorial-particiones (3 particiones)
                                                    │
                                                    ▼
                       KafkaMessageConsumer (@KafkaListener, group: mi-grupo-educativo)
                                                    │
                                                    ▼
                              log de ms-pedidos360-kafka
```

| Elemento | Valor |
| --- | --- |
| Topic | `tutorial-particiones` |
| Particiones | 3 (ver nota más abajo) |
| Grupo de consumidores | `mi-grupo-educativo` |
| Puerto de la app | `8888` |
| Kafka (host / contenedores) | `localhost:9092` / `kafka:29092` |
| Kafka UI | `http://localhost:8086` (el 8080 lo usa el BFF) |
| ZooKeeper | `localhost:2181` |

La `key` define la partición destino mediante el hash `murmur2` de Kafka: la misma
key siempre cae en la misma partición. Si se omite, Kafka reparte por round-robin.

---

## Requisitos

- Java 21 y Maven (o el `./mvnw` incluido en el microservicio).
- Docker + Docker Compose.
- Variables de entorno del repo: `cp .env.example .env` (el compose las necesita
  aunque solo levantes Kafka).

---

## Cómo levantar el entorno

**1. Infraestructura y microservicio (todo desde la raíz del repo):**

```bash
cp .env.example .env          # si aún no existe
docker-compose up -d zookeeper kafka kafka-ui ms-pedidos360-kafka
```

**2. (Opcional) Crear el topic con 3 particiones antes del primer mensaje:**

El broker tiene `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`, por lo que el topic se crea
solo. Si quieres ver el reparto en 3 particiones como en la guía, créalo antes de
enviar mensajes:

```bash
docker compose exec kafka kafka-topics --create \
  --topic tutorial-particiones --partitions 3 --replication-factor 1 \
  --if-not-exists --bootstrap-server localhost:29092
```

**3. O ejecutar el microservicio en local (sin contenedor de la app):**

```bash
cd ms-pedidos360-kafka
cp .env-example .env
./mvnw spring-boot:run
```

**4. Comprobar que responde** (da HTTP 400 porque falta `message`, pero confirma
que la app está arriba):

```bash
curl -i http://localhost:8888/api/kafka/send
```

---

## Endpoints

| Método | Ruta | Parámetros | Descripción |
| --- | --- | --- | --- |
| `POST` / `GET` | `/api/kafka/send` | `message` (obligatorio), `key` (opcional) | Publica un mensaje en el topic. |

Ejemplos:

```bash
curl -X POST "http://localhost:8888/api/kafka/send?message=hola&key=orden-1"
curl "http://localhost:8888/api/kafka/send?message=Paso1&key=orden-555"
```

Respuesta:

```
Mensaje enviado: hola | Key: orden-1
```

---

## Scripts de carga

Están en `ms-pedidos360-kafka/`. Generan keys rotativas (`orden-0`, `orden-1`, ...),
reenvían N peticiones al endpoint y muestran total, OK, fallidos, tiempo y msg/s.

### Linux / macOS / Git Bash / WSL (`send-many.sh`)

```bash
cd ms-pedidos360-kafka
chmod +x send-many.sh          # solo la primera vez
./send-many.sh                 # 100 mensajes, secuencial
./send-many.sh 500             # 500 mensajes
./send-many.sh 1000 20         # 1000 mensajes con 20 peticiones en paralelo
```

Variables de entorno opcionales:

| Variable | Default | Descripción |
| --- | --- | --- |
| `URL` | `http://localhost:8888/api/kafka/send` | Endpoint destino |
| `COUNT` | `100` | Cantidad de mensajes |
| `PARALLEL` | `1` | Peticiones simultáneas |
| `KEY_POOL` | `12` | Keys distintas (reparto entre particiones) |
| `KEY_PREFIX` / `MSG_PREFIX` | `orden` / `mensaje` | Prefijos de key y mensaje |
| `METHOD` | `POST` | `GET` o `POST` |
| `TIMEOUT` | `15` | Timeout por petición (segundos) |
| `DELAY` | `0` | Segundos de espera entre envíos por worker |
| `VERBOSE` | `0` | `1` muestra cada petición |

Ejemplos: `PARALLEL=10 ./send-many.sh 2000`, `KEY_POOL=3 PARALLEL=5 ./send-many.sh 300`,
`METHOD=GET ./send-many.sh 50`.

### Windows PowerShell (`send-many.ps1`)

```powershell
cd ms-pedidos360-kafka
.\send-many.ps1                          # 100 mensajes, secuencial
.\send-many.ps1 -Count 500
.\send-many.ps1 -Count 1000 -Parallel 20 # -Parallel requiere PowerShell 7+
```

Si la política de ejecución lo bloquea:

```powershell
powershell -ExecutionPolicy Bypass -File .\send-many.ps1 -Count 500
```

Parámetros: `-Count`, `-Parallel`, `-Url`, `-KeyPool`, `-KeyPrefix`, `-MsgPrefix`,
`-Method`, `-Timeout`, `-VerboseOut` (mismos valores por defecto que el script bash).

---

## Cómo ver los resultados

**1. Log del consumidor:** cada mensaje aparece con su partición, offset y key:

```bash
docker logs -f ms-pedidos360-kafka | grep "MENSAJE RECIBIDO"
```

**2. Kafka UI:** `http://localhost:8086` → topic `tutorial-particiones` → mensajes.

**3. Consola de Kafka dentro del contenedor:**

```bash
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 --topic tutorial-particiones \
  --from-beginning --property print.key=true --property print.partition=true
```

---

## Problemas comunes

| Síntoma | Solución |
| --- | --- |
| El endpoint no responde | Verifica `docker ps` y que `ms-pedidos360-kafka` esté arriba en 8888. |
| `Connection refused` a `localhost:9092` | El broker tarda en arrancar; espera el healthcheck o usa `docker compose up -d kafka`. |
| `Unknown topic` o no llegan mensajes | El topic se crea solo; créalo con 3 particiones con el comando de arriba. |
| `TimeoutException` al publicar | Revisa los `advertised.listeners`: dentro de Docker el cliente debe usar `kafka:29092`, no `localhost:9092`. |
| No se ve el reparto por particiones | Usa varias keys (`KEY_POOL>1`) y crea el topic con 3 particiones antes de enviar. |
| Conflicto de puerto 8080 | La UI está en 8086; 8080 pertenece al BFF. |

---

## Estructura del microservicio

```
ms-pedidos360-kafka/
├── pom.xml                    # Spring Boot 4.1.1, Java 21, starters webmvc/kafka/validation
├── Dockerfile                 # build Maven -> JRE 21 (EXPOSE 8888)
├── mvnw / .mvn/               # Maven Wrapper (igual que los otros microservicios)
├── .env-example               # KAFKA_BOOTSTRAP_SERVERS / TOPIC / GROUP_ID
├── send-many.sh               # script de carga (Linux/macOS/Git Bash/WSL)
├── send-many.ps1              # script de carga (Windows PowerShell)
└── src/main/java/com/pedidos360/kafka/
    ├── KafkaApplication.java
    ├── config/{KafkaConfig,KafkaProperties}.java
    ├── controller/KafkaController.java
    ├── producer/KafkaMessageProducer.java
    └── consumer/KafkaMessageConsumer.java
```

Tests (8): `KafkaApplicationTests`, `KafkaControllerTest`, `KafkaMessageProducerTest`,
`KafkaMessageConsumerTest` y `KafkaIntegrationTest` (broker embebido con 3 particiones).

```bash
cd ms-pedidos360-kafka && ./mvnw test
```
