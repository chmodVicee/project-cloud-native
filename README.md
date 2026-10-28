# Pedidos360 - Arquitectura de Microservicios

Sistema de gestión de pedidos con arquitectura de microservices. La autenticación y autorización centralizada en un BFF que valida el JWT emitido por el login de Microsoft Entra ID (OAuth2) y enruta las peticiones a los microservicios internos.

```
Microsoft Entra ID (Usuarios + Roles)
   │  OAuth2 (Authorization Code + PKCE)
   ▼
[ SPA React: FRONTEND ]  ← aplicación de usuario
   │  login local JWT + botón "Ingresar con Microsoft"
   ▼
[ BFF: ms-pedidos360-bff ]  ← exposición de la API
   │  Valida JWT + autorización (ADMIN/USER) + CORS
   ├──► ms-pedidos360-users    (usuarios, auth OAuth2, registro/login JWT)
   ├──► ms-pedidos360-catalog  (categorías y productos)
   └──► ms-pedidos360-orders   (órdenes, reglas de negocio, stock)
               │
               └──► ms-pedidos360-catalog (valida precio y descuenta stock)
```

> En **AWS** el mismo conjunto completa se despliega con **API Gateway + ALB + ECS
> (Fargate)**; SPA y API viven bajo el mismo origen del API Gateway (ver
> [Despliegue en AWS](#despliegue-en-aws-api-gateway--ecs-fargate)).

## Componentes

| Componente                 | Puerto local | BD          | Función |
|----------------------------|--------------|-------------|---------|
| **FRONTEND** (React + Vite) | 4200         | -           | SPA: catálogo, carrito, órdenes y panel admin |
| **ms-pedidos360-bff**      | 8080 (público) | -         | Validación de JWT, roles, CORS y ruteo hacia los servicios internos |
| **ms-pedidos360-users**    | 8083         | `users_db`  | Usuarios, login OAuth2 con Microsoft Entra ID y emisión de JWT |
| **ms-pedidos360-catalog**  | 8081         | `catalog_db`| CRUD de categorías y productos, control de stock |
| **ms-pedidos360-orders**   | 8082         | `orders_db` | Órdenes, estados y descuento de stock contra el catálogo |
| **RabbitMQ**               | 5672 / 15672 | -           | Broker de mensajería asíncrona (AMQP + consola web de administración) |
| **Pub/Sub emulator**       | 8085         | -           | Emulador local de Google Cloud Pub/Sub (sin credenciales de GCP) |

**Tecnologías:** Java 21 · Spring Boot · PostgreSQL · RabbitMQ · Google Cloud Pub/Sub · JWT · React + Vite · Maven · Docker / Docker Compose · AWS (API Gateway, ALB, ECR, ECS Fargate, CloudFormation)

---

## Ejecución con Docker (recomendada)

Levanta PostgreSQL, RabbitMQ, el emulador de Pub/Sub y los servicios de la aplicación (SPA + microservicios) en un solo comando:

```bash
# 1) Copiar el archivo de variables y completar los valores:
cp .env.example .env

# 2) Levantar todo:
docker-compose up --build
```

Una vez arriba:

* Aplicación web: `http://localhost:4200`
* BFF (punto de entrada único de la API): `http://localhost:8080`
* Login con Microsoft (OAuth2): `http://localhost:8080/oauth2/authorization/azure`
* Catálogo (vía BFF): `http://localhost:8080/api/catalog/products`
* Consola de RabbitMQ: `http://localhost:15672` (usuario/clave = `RABBITMQ_USER` / `RABBITMQ_PASSWORD`)
* Los puertos 8081, 8082 y 8083 se exponen únicamente para depuración; la aplicación debe consumir siempre el 8080.

> Las variables requeridas en `.env`: `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID`, `JWT_SECRET`. `SPA_BASE_URL` define hacia dónde redirige el backend tras el login OAuth2 (por defecto `http://localhost:4200`).

### Parar todo
```bash
docker-compose down
```

> Si actualizas desde una versión anterior, elimina el volumen de la base para que se creen las nuevas bases `catalog_db` y `orders_db`: `docker-compose down -v`

---

## Ejecución local (sin Docker)

Cada microservicio necesita PostgreSQL con su base de datos (`users_db`, `catalog_db`, `orders_db`). Requiere un archivo `.env` en cada carpeta usando su `.env-example`:

```bash
cd ms-pedidos360-users && ./mvnw spring-boot:run      # puerto 8083
cd ms-pedidos360-catalog && ./mvnw spring-boot:run    # puerto 8081
cd ms-pedidos360-orders && ./mvnw spring-boot:run     # puerto 8082
cd ms-pedidos360-bff && ./mvnw spring-boot:run        # puerto 8080
cd FRONTEND && npm install && npm run dev             # puerto 4200
```

> En modo local, definí `SPA_BASE_URL=http://localhost:4200` en `ms-pedidos360-users` y `VITE_BFF_URL=http://localhost:8080` en la SPA (el proxy de Vite ya deriva `/api` y `/oauth2` al BFF por defecto).

---

## Despliegue en AWS (API Gateway + ECS Fargate)

> **Nota:** por ahora el proyecto se ejecuta solo en local (docker-compose). Esta sección
> se conserva como referencia y no incluye la mensajería asíncrona.

En AWS, todo el backend corre en **ECS Fargate** (un task con 6 contenedores tras un
ALB) y el **API Gateway HTTP** es el único punto público de entrada. La SPA se sirve
por nginx dentro del mismo task, por lo que **SPA y API comparten el mismo origen
HTTPS**.

```
[ SPA React + API ]  →  https://<api-id>.execute-api.us-east-1.amazonaws.com
        │  API Gateway HTTP ($default: reenvía el path completo)
        ▼
[ ALB (HTTP :80) ]  →  /api/*, /oauth2/*, /login/*  →  BFF (bff:8080)
                     →  cualquier otra ruta            →  frontend (nginx SPA)
        ▼
[ ECS Fargate: 1 task con 6 contenedores (misma red localhost) ]
     ├─ db        postgres:16 + init.sql (users_db, catalog_db, orders_db)
     ├─ users     8083  (OAuth2 Microsoft/Google + JWT HS512)
     ├─ catalog   8081
     ├─ orders    8082  (descuenta stock contra catalog)
     ├─ bff       8080  (valida JWT, autoriza por rol y enruta)
     └─ frontend  80    (nginx sirve la SPA React build)
```

### Conceptos clave

- **API Gateway (AWS Api Gateway v2)**: ruta `$default` en HTTP_PROXY al ALB con el
  path completo; expone la SPA y toda la API bajo la URL `https://<api>....`.amazonaws.com`.
- **ALB**: reglas por path — `/api/*`, `/oauth2/*`, `/login/*` van al contenedor
  `bff` (8080); el resto va al contenedor `frontend` (nginx, puerto 80).
- **ECS Fargate**: un service corre **1 task = 6 contenedores** que comparten
  `localhost`; el BFF alcanza a los microservicios igual que en docker-compose local
  (`USERS_URL=http://localhost:8083`, `CATALOG_URL=http://localhost:8081`,
  `ORDERS_URL=http://localhost:8082`, `DB_HOST=localhost`).
- **Orden de arranque**: los contenedores `users`, `catalog` y `orders` esperan a que
  `db` esté sano (`DependsOn` + health check `pg_isready`).
- **BFF_PUBLIC_BASE / CORS_ALLOWED_ORIGIN / SPA_BASE_URL / AZURE_REDIRECT_URI** se
  fijan automáticamente a la URL del API Gateway.

### Desplegar (una sola vez)

```bash
chmod +x deploy/aws/*.sh
./deploy/aws/deploy.sh            # usa la región de aws configure
./deploy/aws/deploy.sh us-east-1  # o fuerza una región
```

El script:
1. `push-images.sh` → crea los 6 repos ECR (`pedidos360-db/users/catalog/orders/bff/frontend`)
   y sube las imágenes.
2. `cloudformation deploy` → VPC, subredes, ALB, ECS (task de 6 contenedores) y API
   Gateway HTTP.
3. Espera al stack e imprime la **URL del API Gateway** y los **Redirect URI** de
   Microsoft y Google.

> **Redirect URI en Azure y Google:** después del despliegue, agrega los valores impresos
> (`.../login/oauth2/code/azure` en el App Registration de Microsoft, y
> `.../login/oauth2/code/google` en Google Cloud Console).

### Verificación rápida

```bash
API_URL=$(aws cloudformation describe-stacks --stack-name pedidos360-stack \
  --query "Stacks[0].Outputs[?OutputKey=='ApiGatewayUrl'].OutputValue" --output text)

curl -i "$API_URL"                            # SPA (index.html)
curl -i "$API_URL/api/catalog/products"       # 401 sin token (JWT se valida en BFF)
curl -I "$API_URL/oauth2/authorization/azure" # 302 → login Microsoft
```

### Comandos útiles

```bash
aws logs tail /ecs/pedidos360 --follow                      # logs de los contenedores
aws ecs describe-services --cluster pedidos360 --services pedidos360-svc
aws cloudformation delete-stack --stack-name pedidos360-stack   # borrar todo
```

> Guía completa del despliegue (incluida la nota del rol `LabRole` del laboratorio):
> [`deploy/aws/README.md`](deploy/aws/README.md).

---

## Frontend (React + Vite)

SPA en `FRONTEND/` que consume la API **solo a través del BFF**. En desarrollo usa el
proxy de Vite (deriva `/api` y `/oauth2` al BFF) y guarda el JWT en `localStorage`.
En AWS (`VITE_API_URL` vacío) consume rutas relativas contra el mismo origen del API
Gateway.

**Pantallas:**

| Ruta | Acceso | Descripción |
|------|--------|-------------|
| `/login` | público | Login/registro local + botones "Ingresar con Microsoft" y "Ingresar con Google" |
| `/auth/callback` | público | Recibe el `token` que el backend adjunta tras el login OAuth2 |
| `/` | autenticado | Catálogo con filtro por categoría y alta al carrito |
| `/carrito` | autenticado | Cantidades, total y creación de orden |
| `/mis-pedidos` | autenticado | Órdenes del usuario con sus items |
| `/admin/productos` | ADMIN | CRUD de productos y categorías |
| `/admin/ordenes` | ADMIN | Cambio de estado de cualquier orden (único origen de los cambios de estado) |
| `/admin/usuarios` | ADMIN | Listado de usuarios |

**Flujo OAuth2:** el botón de Microsoft lleva a `/oauth2/authorization/azure` (y el de Google a `/oauth2/authorization/google`). El proveedor redirige a su `AZURE_REDIRECT_URI`/`GOOGLE_REDIRECT_URI`, y `ms-pedidos360-users` completa el login y redirige a `${SPA_BASE_URL}/auth/callback?token=...`, donde la SPA guarda el token y navega al catálogo.

---

### Estados de un pedido

`RECIBIDO → CONFIRMADO → PREPARANDO → LISTO → ENTREGADO` (o `CANCELADO`). Un pedido
nace en `RECIBIDO` y avanza **solo** cuando un ADMIN cambia su estado desde
`/admin/ordenes` (`PUT /api/orders/{id}/status`). Desde `ENTREGADO` o `CANCELADO` ya no
se puede cambiar. Las pantallas de pedidos se refrescan cada 4 s para reflejar el cambio.

> La simulación automática anterior (`OrderStatusSimulator`, cambio cada 6 s y
> cancelación aleatoria, variables `ORDERS_SIMULATION_*`) fue eliminada.

---

## Reglas de autorización (BFF)

* **Público (sin JWT):** `/oauth2/**`, `/login/oauth2/**`, `/api/auth/**` (login/registro).
* **Cualquier usuario autenticado:** lectura del catálogo, sus órdenes, su perfil.
* **Solo ADMIN:** escritura en catálogo (`POST/PUT/PATCH/DELETE /api/catalog/**`), cambio
  de estado de órdenes (`PUT /api/orders/{id}/status`) y listado de usuarios (`GET /api/users`).

Respuestas de rechazo: `401` sin token o con token inválido/expirado, `403` con token
válido pero sin rol suficiente.

Los microservicios internos confían en la red interna; el BFF agrega el encabezado
`X-User-Email` para las órdenes.

### ¿Quién es ADMIN?

* Los usuarios que inician con **Microsoft** y cuyo correo termina en `@duocuc.cl`
  quedan con rol **ADMIN** automáticamente (configurable con `AZURE_ADMIN_DOMAIN`).
* El **login local** (usuario/contraseña) y el inicio con **Google** crean usuarios con
  rol **USER**. Si querés un ADMIN local, cambia el rol en la base `users_db`.

---

**Variables de Entorno**

```env
JWT_SECRET=tu_clave_hexadecimal_de_64_bytes
JWT_EXPIRATION=86400000
DB_USER=postgres
DB_PASSWORD=postgres
AZURE_CLIENT_ID=
AZURE_CLIENT_SECRET=
AZURE_TENANT_ID=
AZURE_REDIRECT_URI=http://localhost:8080/login/oauth2/code/azure
AZURE_ADMIN_DOMAIN=duocuc.cl          # dominio que recibe rol ADMIN al entrar por Microsoft
GOOGLE_CLIENT_ID=                    # opcional: login con Google
GOOGLE_CLIENT_SECRET=
GOOGLE_REDIRECT_URI=http://localhost:8080/login/oauth2/code/google
RABBITMQ_USER=pedidos360              # usuario del broker local (docker-compose)
RABBITMQ_PASSWORD=cambia_esta_clave
GCP_PUBSUB_ENABLED=true               # false = no se reenvían estados a Pub/Sub
GCP_PROJECT_ID=pedidos360-local       # proyecto (ficticio en el emulador)
PUBSUB_EMULATOR_HOST=pubsub-emulator:8085   # vacío = Pub/Sub real con credenciales ADC
```

> _En funcionamiento: **Login con Microsoft Entra ID (OAuth2 + PKCE)**, **emisión/validación de JWT con roles**, **arquitectura BFF + 3 microservicios con bases de datos independientes**, **despliegue controlado por AWS API Gateway sobre ECS Fargate**._