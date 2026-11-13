# Pedidos360 · Cómo iniciar y desplegar

Guía paso a paso con los comandos exactos. Hay dos formas de levantar el proyecto:

| Forma | Cuándo usarla | Tiempo |
|---|---|---|
| [A. En AWS (EC2)](#a-despliegue-en-aws-ec2) | Presentación y prueba completa | 15 a 20 min |
| [B. En el computador](#b-todo-en-el-computador-plan-b) | Desarrollo, o si AWS falla | 5 min |

El detalle del backend (topología de RabbitMQ, ACK/NACK, API del administrador) está en [back-app/README.md](back-app/README.md).

| Carpeta | Contenido |
|---|---|
| `back-app/` | 4 microservicios Spring Boot: `productos-catalogo` (8081), `venta-carrito` (8082), `rabbitmq-admin` (8083), `kafka-admin` (8084) |
| `docker/` | Clúster RabbitMQ (3 nodos), Kafka (Zookeeper + 3 brokers) y Kafka-UI; los usan tanto el entorno local como la EC2 |
| `front-app/` | Frontend Angular (4200) |
| `infraestructura/` | Terraform y scripts de despliegue en la EC2 |

---

## Antes de empezar

- **Todo lo que se despliega sale de GitHub.** La EC2 clona la rama `main`, así que antes de desplegar:
  ```bash
  git status          # no debe quedar nada pendiente
  git push
  ```
- Los comandos de la forma A se ejecutan en **Git Bash** (no en PowerShell), porque `credenciales.sh` es un script de Bash.
- El acceso a la EC2 queda abierto **solo para la IP desde la que se ejecuta `terraform apply`**. Hay que ejecutarlo conectado a la red donde se va a presentar. Si se cambia de red, basta repetir `terraform apply`.

---

## A. Despliegue en AWS (EC2)

### 1. Credenciales de AWS Academy

1. Entrar a AWS Academy → **Learner Lab** → **Start Lab** y esperar el círculo verde.
2. **AWS Details** → **AWS CLI: Show**. Copiar los tres valores.
3. Pegarlos en `infraestructura/terraform/credenciales.sh`, que debe quedar así:
   ```bash
   export AWS_ACCESS_KEY_ID=...
   export AWS_SECRET_ACCESS_KEY=...
   export AWS_SESSION_TOKEN=...
   ```
   Las credenciales cambian cada vez que se inicia el laboratorio. Este archivo no se sube a GitHub.

### 2. Crear la infraestructura

En Git Bash, desde la raíz del repositorio:

```bash
cd infraestructura/terraform
source credenciales.sh
terraform init
terraform apply
```

Escribir `yes` cuando lo pida. Al terminar imprime:

```
instance_public_ip = "X.X.X.X"
rabbitmq_dashboard = "http://X.X.X.X:15672"
ssh                = "ssh -i pedidos360.pem ubuntu@X.X.X.X"
```

Anotar la IP: se usa en todos los pasos siguientes (aparece como `IP`). Para verla de nuevo: `terraform output`.

> Terraform usa el key pair `pedidos360`. Si en la cuenta solo existe `vockey`:
> `terraform apply -var "key_name=vockey"`

### 3. Esperar el despliegue (8 a 10 minutos)

La instancia (`t3.large`, 8 GB) instala Docker, clona el repositorio, compila los 4 microservicios y los levanta junto al clúster RabbitMQ (3 nodos), Kafka (Zookeeper + 3 brokers + Kafka-UI) y MySQL, usando `infraestructura/docker-compose.yml`. Está listo cuando estas direcciones responden en el navegador:

| Dirección | Debe mostrar |
|---|---|
| `http://IP:8081/api/productos/ping` | Texto de servicio activo |
| `http://IP:8082/api/carrito/ping` | `venta-carrito activo` |
| `http://IP:8083/swagger-ui.html` | Documentación del administrador |
| `http://IP:8084/swagger-ui.html` | Documentación de kafka-admin |
| `http://IP:15672` | Dashboard de RabbitMQ (usuario `admin`, clave `admin123`), con los 3 nodos en *Overview* |
| `http://IP:8090` | Kafka-UI: 3 brokers y los tópicos `pedidos360.ventas.confirmadas` y `pedidos360.inventario.actualizado` (más sus `.DLT`) |

En el dashboard, pestaña **Queues and Streams**, deben aparecer `pedidos.stock.queue`, `pedidos.ticket.queue`, `pedidos.stock.dlq` y `pedidos.ticket.dlq`.

### 4. Frontend

1. Editar `front-app/src/environments/environment.development.ts` y poner la IP:
   ```ts
   catalogoUrl: 'http://IP:8081',
   carritoUrl: 'http://IP:8082',
   ```
2. En una terminal:
   ```bash
   cd front-app
   npm install        # solo la primera vez
   npm start
   ```
3. Abrir `http://localhost:4200`.

No subir a GitHub el cambio de IP: es distinta en cada despliegue. Para deshacerlo: `git checkout front-app/src/environments/environment.development.ts`

### 5. Ver los logs de los microservicios

Abrir dos terminales de Git Bash en la carpeta donde está `pedidos360.pem`. En cada una, entrar a la instancia:

```bash
ssh -i pedidos360.pem ubuntu@IP
```

Y dejar corriendo un log en cada terminal:

```bash
docker logs -f productos-catalogo     # terminal 1: stock y su DLQ
docker logs -f venta-carrito          # terminal 2: productor, tickets y su DLQ
```

Para Kafka, buscar en esos mismos logs `KAFKA-PRODUCTOR`, `KAFKA-CONSUMIDOR` y `KAFKA-DLT`:

```bash
docker logs productos-catalogo 2>&1 | grep KAFKA
```

Se sale del log con `Ctrl + C` y de la instancia con `exit`.

### 6. Apagar todo al terminar

```bash
cd infraestructura/terraform
source credenciales.sh
terraform destroy
```

Escribir `yes`. Así no se gasta el saldo del laboratorio.

---

## Qué mostrar en la presentación

### 1. Flujo completo (productor → exchange → consumidores)

1. En `http://localhost:4200`, iniciar sesión con la cuenta de Azure AD.
2. Agregar productos al carrito, ir a **Carrito** y pulsar **Continuar compra**. Aparece el número de pedido.
3. En los logs:
   - `venta-carrito`: `[PRODUCTOR] Pedido ... publicado` y `[TICKET] Comprobante generado`
   - `productos-catalogo`: `[STOCK] OK pedido ...: stock descontado -> ACK`

   Cómo leer una línea del log, por ejemplo
   `2026-10-12T18:47:21Z  INFO ... [venta-carrito] ... TicketConsumer : [TICKET] OK pedido 144c5a6a-... -> ACK`:

   | Parte | Significado |
   |---|---|
   | `2026-10-12T18:47:21Z` | Fecha y hora en UTC (3 horas más que en Chile) |
   | `INFO` | Nivel informativo. Un problema aparece como `WARN` o `ERROR` |
   | `[venta-carrito]` y `TicketConsumer` | El microservicio y la clase que escribieron la línea |
   | `pedido 144c5a6a-...` | El id completo del pedido. Sus primeros 8 caracteres son el número que ve el usuario en pantalla |
   | `-> ACK` | El consumidor confirmó el mensaje a RabbitMQ y este salió de la cola |
4. En el dashboard (`http://IP:15672`), pestaña **Queues and Streams**: los contadores quedan en 0 porque los consumidores procesan el mensaje en milisegundos. Para ver que pasó, hacer clic en `pedidos.stock.queue` y, en el gráfico **Message rates**, elegir **Last ten minutes**: aparece un pico de *Publish* y de *Consumer ack* por cada pedido.

Para ver un mensaje detenido en la cola, apagar un consumidor por SSH antes de confirmar el pedido:

```bash
docker stop productos-catalogo
```

Al confirmar un pedido, `pedidos.stock.queue` muestra **Ready = 1**. Luego hay dos caminos:

- `docker start productos-catalogo` antes de 60 segundos: el mensaje se consume y vuelve a 0.
- Esperar 60 segundos: el mensaje vence (TTL) y pasa a `pedidos.stock.dlq`. Al volver a iniciar el servicio, el log muestra `[DLQ] ... motivo: expired`.

### 2. Mensaje fallido que termina en la DLQ

1. En el dashboard: **Exchanges** → `pedidos.exchange` → **Publish message**.
2. Completar:
   - **Routing key**: `pedido.creado`
   - **Properties**: `content_type` = `application/json`
   - **Payload** (producto que no existe):
     ```json
     {"pedidoId":"DEMO-1","usuarioId":"demo","total":9980,
      "items":[{"productoId":999,"nombreProducto":"No existe","cantidad":2,"precioUnitario":4990}]}
     ```
3. Pulsar **Publish message**. En el log de `productos-catalogo`:
   - `[STOCK] Error NO recuperable ... -> NACK sin reencolar -> DLQ`
   - `[DLQ] Mensaje en cuarentena | cola origen: pedidos.stock.queue | motivo: rejected`

Otra variante: publicar el texto `hola` como payload (mensaje inválido) y llega a las dos DLQ.

### 3. Microservicio administrador de RabbitMQ

Todo se hace con Postman. Las peticiones ya vienen armadas en una colección: no hay que escribir ninguna, solo abrirlas y pulsar **Send**.

#### a) Copiar el JWT

1. En el navegador, con la sesión iniciada en `http://localhost:4200`, pulsar `F12`.
2. Pestaña **Network** y recargar la página del carrito.
3. Hacer clic en una llamada al backend (por ejemplo la que va a `IP:8082/api/carrito/...`).
4. En **Headers** → **Request Headers**, buscar `Authorization`.
5. Copiar el valor **sin** la palabra `Bearer` ni el espacio. Empieza con `eyJ`.

El token dura cerca de una hora. Si las peticiones empiezan a responder 401, copiar uno nuevo.

#### b) Importar la colección (solo la primera vez)

1. En Postman, pulsar **Import** (arriba a la izquierda).
2. Arrastrar el archivo `back-app/postman/Pedidos360-backend.postman_collection.json`.
3. En el panel izquierdo aparece **Pedidos360 - Backend (RabbitMQ)** con cuatro carpetas.

#### c) Poner la IP y el token en las variables

La IP **no** se escribe en la barra de direcciones de cada petición. Las peticiones usan comodines como `{{adminUrl}}`, y Postman los reemplaza por lo que se defina aquí:

1. En el panel izquierdo, hacer clic en el nombre **Pedidos360 - Backend (RabbitMQ)** (el título de la colección, no una carpeta ni una petición).
2. En la pestaña que se abre, hacer clic en **Variables**.
3. Hacer doble clic en el valor de cada fila y reemplazarlo (si hay dos columnas, usar **Current value**):

   | Variable | Valor |
   |---|---|
   | `catalogoUrl` | `http://IP:8081` |
   | `carritoUrl` | `http://IP:8082` |
   | `adminUrl` | `http://IP:8083` |
   | `token` | el JWT copiado |
   | `usuarioId` | no se cambia |

   Sin espacios y sin `/` al final.
4. Pulsar **Save** (`Ctrl + S`). Sin guardar, se siguen usando los valores anteriores.

Si por error se modificó la URL de una petición: cerrar su pestaña y elegir **Don't save**.

#### d) Cómo se ejecuta una petición

1. En el panel izquierdo, abrir una carpeta con la flecha `>` y hacer clic en la petición.
2. No cambiar nada. Pulsar el botón azul **Send**.
3. Abajo, en **Response**, aparece el código de estado (por ejemplo `201 Created`) y la respuesta.

Para comprobar que las variables quedaron bien: **1. Catalogo → Listar productos** debe responder **200** con los 5 productos.

#### e) Crear los recursos

Carpeta **3. Administrador RabbitMQ**, en este orden. Después de cada **Send**, recargar el dashboard (`F5`) para ver el recurso nuevo.

| # | Petición | Responde | Qué aparece en el dashboard (`http://IP:15672`) |
|---|---|---|---|
| 1 | Crear cola con DLQ | 201 | **Queues and Streams**: `notificaciones.email.queue` y `notificaciones.email.queue.dlq` |
| 2 | Crear exchange | 201 | **Exchanges**: `notificaciones.exchange` |
| 3 | Crear binding | 201 | **Exchanges** → clic en `notificaciones.exchange` → **Bindings**: la cola enlazada con `notificacion.email` |

#### f) Mostrar una validación

Carpeta **4. Validaciones del administrador (deben fallar)** → **Cola con nombre vacio -> 400** → **Send**.

Responde **400 Bad Request** con el detalle del error. Que falle es lo correcto: demuestra que la API no permite crear una cola sin nombre. El código esperado de cada petición de esta carpeta está en su nombre.

#### g) Eliminar lo creado

De vuelta en la carpeta **3. Administrador RabbitMQ**, en este orden:

| # | Petición | Responde |
|---|---|---|
| 1 | Eliminar binding | 204 |
| 2 | Eliminar exchange | 204 |
| 3 | Eliminar cola | 204 |
| 4 | Eliminar DLQ de la cola | 204 |
| 5 | Eliminar DLX de la cola | 204 |

El 204 significa "eliminado" y la respuesta sale vacía. Al recargar el dashboard, en **Queues and Streams** quedan solo las 4 colas `pedidos.*`.

#### Si una petición responde distinto

| Código | Causa | Solución |
|---|---|---|
| 401 | El token venció o quedó mal pegado | Copiar uno nuevo en la variable `token` y guardar |
| 409 al crear | El recurso ya existe de una prueba anterior | Ejecutar primero las peticiones de eliminar |
| 404 al eliminar | Ya estaba eliminado | Nada, seguir con la siguiente |
| `Could not send request` | Una URL quedó mal escrita | Revisar las variables del paso c) |

### 4. Código que respalda la pauta

| Qué mostrar | Archivo |
|---|---|
| Nombres centralizados | `back-app/venta-carrito/src/main/resources/application.yml` (bloque `pedidos360.rabbitmq`) |
| Beans Queue, Exchange y Binding | `config/RabbitMQConfig.java` de catálogo y de carrito |
| Consumidor con ACK/NACK manual | `messaging/pedidos/StockPedidoConsumer.java` (catálogo) |
| Registro de la DLQ en logs | `messaging/dlq/DlqAuditoriaConsumer.java` |
| API REST del administrador | `rabbitmq-admin/.../controller/RabbitAdminController.java` |
| Lógica encapsulada en un servicio | `rabbitmq-admin/.../service/RabbitAdminService.java` |
| Validación de entrada | `rabbitmq-admin/.../dto/CrearColaRequest.java` |

---

## B. Todo en el computador (plan B)

Requisitos: Docker Desktop abierto, Java 17 o superior, Maven y Node.

### 1. RabbitMQ y MySQL

Desde la raíz del repositorio:

```bash
docker compose up -d
docker compose ps        # los dos contenedores deben estar "Up"
```

### 2. Los tres microservicios

Una terminal por cada uno. La terminal queda ocupada mostrando los logs; está listo cuando aparece `Started ...Application`.

```bash
cd back-app/productos-catalogo
mvn spring-boot:run
```

```bash
cd back-app/venta-carrito
mvn spring-boot:run
```

```bash
cd back-app/rabbitmq-admin
mvn spring-boot:run
```

Se detienen con `Ctrl + C`.

### 3. Frontend

`front-app/src/environments/environment.development.ts` debe apuntar a `localhost` (es el valor que viene en el repositorio):

```ts
catalogoUrl: 'http://localhost:8081',
carritoUrl: 'http://localhost:8082',
```

```bash
cd front-app
npm install        # solo la primera vez
npm start
```

Abrir `http://localhost:4200`. La demostración es la misma de arriba cambiando `IP` por `localhost`, y los logs se ven directamente en las terminales de cada microservicio.

### 4. Apagar

`Ctrl + C` en cada terminal y luego, en la raíz:

```bash
docker compose down
```

---

## Actualizar la EC2 después de un cambio

Sin recrear la instancia: hacer `git push`, entrar por SSH y ejecutar:

```bash
cd /opt/pedidos360 && sudo git pull && sudo bash infraestructura/desplegar.sh
```

---

## Si algo falla

| Síntoma | Qué revisar |
|---|---|
| `terraform apply` dice que las credenciales no son válidas o expiraron | Reiniciar el laboratorio, copiar las credenciales nuevas y repetir `source credenciales.sh` |
| `terraform apply` falla con `InvalidKeyPair.NotFound` | El key pair no existe en la cuenta: usar `-var "key_name=vockey"` |
| Las direcciones de la EC2 no cargan después de 10 minutos | Entrar por SSH y ejecutar `sudo cat /var/log/arranque.log` (al final debe decir `ARRANQUE COMPLETO`) y `docker ps` (con `docker ps -a` deben verse 14 contenedores: 13 en *Up* y `kafka-init` en *Exited (0)*, porque solo crea los tópicos) |
| Dejó de cargar después de cambiar de red | La IP propia cambió: repetir `terraform apply` |
| Un microservicio se reinicia en la EC2 | `docker logs productos-catalogo` (o el que corresponda) muestra el error. Si dice `OOMKilled` (`docker inspect`), revisar la memoria con `free -h` |
| El frontend muestra errores de CORS | La URL del frontend debe ser `http://localhost:4200` |
| El login vuelve con error de `redirect_uri` | Abrir el frontend exactamente en `http://localhost:4200`, que es la URL registrada en Azure AD |
| El backend responde 401 | No hay sesión iniciada o el JWT de Postman venció (dura cerca de 1 hora): copiar uno nuevo |
| En local aparece `PRECONDITION_FAILED - inequivalent arg` | Quedó una cola antigua con otra configuración: eliminarla en el dashboard y reiniciar el microservicio |
| En local un puerto está ocupado | Hay otro proceso usando 8081, 8082, 8083 o 4200: cerrar la terminal anterior |
