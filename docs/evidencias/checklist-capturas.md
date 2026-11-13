# Checklist de capturas – PDF de evidencias EP3

Guardar cada captura en `docs/evidencias/capturas/` con **el nombre exacto** de la tabla. Claude avisa en el momento de sacar cada una y la marca con ✅ cuando la revisa.

> ⚠️ Antes de guardar, tapar o recortar: tokens JWT completos, claves de AWS, contraseñas y el `tenant-id` / `client-id` si se ven enteros.
> La carpeta `capturas/` está en `.gitignore`: las imágenes no se suben a GitHub, solo van en el PDF.
>
> **Logs**: cada microservicio escribe en `logs/<servicio>.log` (raíz del proyecto). Abrir el archivo en VS Code, buscar (Ctrl+F) el texto indicado entre comillas y capturar esas líneas.
> **🔑 = requiere JWT** (token de Azure AD obtenido al iniciar sesión en el frontend).

## 1. Planificación
| ✔ | Archivo | Qué debe verse |
|---|---|---|
| ✅ | *(ya generada)* `docs/planificacion/carta_gantt_EP3_*.png` | Carta Gantt con avance |

## 2. Infraestructura Docker Compose
| ✔ | Archivo | Qué debe verse | Indicador |
|---|---|---|---|
| ✅ | `01_docker_ps.png` | `docker ps` con los 3 nodos RabbitMQ, Zookeeper, 3 brokers Kafka y Kafka-UI en estado *Up/healthy* | Requisitos docker-compose |
| ✅ | `02_rabbitmq_cluster_nodos.png` | Dashboard RabbitMQ → *Overview* con los 3 nodos del clúster | Clúster RabbitMQ |
| ✅ | `03_kafkaui_brokers.png` | Kafka-UI → *Brokers* con los 3 brokers activos | Config. cliente Kafka |
| ✅ | `04_kafkaui_topicos.png` | Kafka-UI → *Topics* con los 2 tópicos y sus DLT | Parámetros de tópicos |
| ✅ | `05_kafkaui_config_ventas.png` | Configuración de `pedidos360.ventas.confirmadas`: 3 particiones, RF 3, `retention.ms`, `cleanup.policy=delete` | Parámetros de tópicos |
| ✅ | `06_kafkaui_config_inventario.png` | Configuración de `pedidos360.inventario.actualizado`: `cleanup.policy=compact` | Parámetros de tópicos |

## 3. Microservicios Kafka
| ✔ | Archivo | Qué debe verse | Indicador |
|---|---|---|---|
| ✅ | `07_log_productor_venta.png` | 🔑 Tras confirmar un carrito: `logs/venta-carrito.log`, buscar "Venta del pedido" (tópico, partición, offset) | Productores |
| ✅ | `08_kafkaui_mensaje_venta.png` | Kafka-UI → Topics → `pedidos360.ventas.confirmadas` → *Messages*: el JSON de `VentaConfirmadaEvent` con su key | Productores con DTO |
| ✅ | `09_log_consumidor_metricas.png` | `logs/productos-catalogo.log`, buscar "KAFKA-CONSUMIDOR": ventas recibidas, "metricas actualizadas" y "ya procesado (idempotencia)" | Consumidores |
| ✅ | `10_postman_mas_vendidos.png` | 🔑 `GET /api/productos/mas-vendidos` con respuesta 200 | Consumidores / flujo de negocio |
| ✅ | `11_log_productor_inventario.png` | `logs/productos-catalogo.log`, buscar "STOCK_DESCONTADO": publicado en `pedidos360.inventario.actualizado-N@offset` | Productores |
| ✅ | `12_log_consumidor_carrito.png` | `logs/venta-carrito.log`, buscar "item(s) de carrito actualizados" | Consumidores |
| ✅ | `13_kafkaui_consumer_groups.png` | Kafka-UI → *Consumers*: los 2 grupos con 3 miembros y estado STABLE | Consumer groups |
| ✅ | `13b_kafkaui_consumer_group_detalle.png` | Kafka-UI → *Consumers* → `catalogo-metricas-ventas` → expandir el tópico: un consumidor distinto por partición y lag 0 | Consumer groups |
| ✅ | `14_rabbitmq_sigue_funcionando.png` | `logs/productos-catalogo.log` buscar "[STOCK]" y `logs/venta-carrito.log` buscar "Comprobante generado" (pedido PED-EP3-0001) | Sin afectar RabbitMQ |
| ✅ | `14b_rabbitmq_colas.png` | Dashboard RabbitMQ → *Queues and Streams*: colas stock/ticket y sus DLQ en *running* con DLX/TTL | Sin afectar RabbitMQ |

## 4. Manejo de errores
| ✔ | Archivo | Qué debe verse | Indicador |
|---|---|---|---|
| ✅ | `15_log_reintentos.png` | `logs/productos-catalogo.log`, buscar "KAFKA-REINTENTO": intentos 1/4 a 4/4 del offset @4 (MySQL detenido) y el "KAFKA-DLT" que sigue | Manejo de errores |
| ✅ | `16_log_mensaje_invalido.png` | Buscar "KAFKA-DLT" en ambos logs: JSON inválido, cantidades negativas y key distinta del productoId | Manejo de errores |
| ✅ | `17_kafkaui_dlt.png` | Kafka-UI → Topics → `pedidos360.ventas.confirmadas.DLT` → *Messages* → abrir un mensaje → pestaña **Headers** (`kafka_dlt-*`) | Manejo de errores |

## 5. Microservicio kafka-admin
| ✔ | Archivo | Qué debe verse | Indicador |
|---|---|---|---|
| ✅ | `18_admin_crear_topico.png` | 🔑 Postman/Swagger: `POST /api/kafka/topicos` → 201 | Administrador Kafka |
| ✅ | `19_admin_listar_topicos.png` | 🔑 `GET /api/kafka/topicos` | Administrador Kafka |
| ✅ | `19b_admin_cluster.png` | 🔑 `GET /api/kafka/cluster`: 3 brokers y controlador | Administrador Kafka |
| ✅ | `20_admin_modificar_config.png` | 🔑 `PUT …/configuraciones` (por ejemplo, cambiar `retention.ms`), con el cambio visible en Kafka-UI | Administrador Kafka |
| ✅ | `20b_kafkaui_config_modificada.png` | Kafka-UI → `pedidos360.notificaciones` → Settings: `retention.ms = 172800000` | Administrador Kafka |
| ✅ | `21_admin_particiones.png` | 🔑 `PATCH …/particiones` | Administrador Kafka |
| ✅ | `21b_admin_particiones_no_reduce.png` | 🔑 `PATCH …/particiones` con un valor menor → 400 "solo se pueden aumentar" | Administrador Kafka |
| ✅ | `22_admin_grupos.png` | 🔑 `GET /api/kafka/grupos/{id}` con offsets y lag | Admin. consumer groups |
| ✅ | `22b_admin_listar_grupos.png` | 🔑 `GET /api/kafka/grupos`: los 2 grupos STABLE | Admin. consumer groups |
| ✅ | `23_admin_eliminar_topico.png` | 🔑 `DELETE /api/kafka/topicos/{nombre}` → 204 | Administrador Kafka |
| ✅ | `23c_admin_no_elimina_internos.png` | 🔑 `DELETE /api/kafka/topicos/__consumer_offsets` → 400 "No se pueden eliminar tópicos internos" | Administrador Kafka |
| ✅ | `24_admin_sin_token_401.png` | Postman sin token: `GET http://localhost:8084/api/kafka/topicos` → 401 | Seguridad JWT |
| ✅ | `24b_admin_prueba_cluster_real.png` | Terminal: `mvn -pl kafka-admin test -Dkafka.local=true -Dtest=KafkaAdminServiceClusterLocalTest` → 8 tests en verde | Administrador Kafka |

## 6. Pruebas unitarias
| ✔ | Archivo | Qué debe verse |
|---|---|---|
| ✅ | `25_tests_backend.png` | `mvn -f back-app/pom.xml test`: Reactor Summary con los 4 microservicios en SUCCESS y BUILD SUCCESS (93 tests en verde, 8 omitidos que requieren el clúster) |
| ✅ | `25b_tests_backend_resumen.png` | `mvn -f back-app/pom.xml test | grep -E "Tests run:|Building |BUILD"`: cada clase de test (incluidas las de Kafka) y el total por módulo: 35 · 32 · 15 · 19 |
| ✅ | `28_tests_frontend.png` | `npx ng test --watch=false`: 12 archivos, 22 tests en verde |

## 7. Despliegue AWS y pruebas de integración
| ✔ | Archivo | Qué debe verse |
|---|---|---|
| ✅ | `29_aws_ec2.png` | Consola AWS: instancia EC2 *running*, t3.large, IP pública |
| ✅ | `30_aws_docker_ps.png` | `docker ps` dentro de la EC2: 14 contenedores (`kafka-init` *Exited (0)*) |
| ✅ | `31_login_idaas.png` | Pantalla de login de Azure AD desde el frontend desplegado. (tenant-id y client-id tapados) |
| ✅ | `32_front_logueado.png` | Frontend con la sesión iniciada |
| ✅ | `33_devtools_bearer.png` | DevTools → *Network*: petición al backend con `Authorization: Bearer …` (token recortado). Versión local lista; opcional repetirla en AWS |
| ✅ | `33b_devtools_api_gateway.png` | DevTools → *Network*: petición a `https://3191rd3t14.execute-api.us-east-1.amazonaws.com/api/productos/mas-vendidos` con 200 y `Authorization: Bearer …` (token recortado): el frontend pasa por el API Manager |
| ✅ | `33c_aws_api_gateway_rutas.png` | Consola AWS → API Gateway → `pedidos360-api` → *Routes*: las 8 rutas `/api/{productos,carrito,rabbitmq,kafka}` |
| ✅ | `34_front_mas_vendidos.png` | Vista "Más vendidos" con datos (contra la EC2) |
| ✅ | `35_front_sin_stock.png` | Carrito con el aviso "Sin stock" y la compra bloqueada |
| ✅ | `35b_aws_logs_flujo_stock.png` | Logs en la EC2: pedido `PED-EP3-AWS-0001` por RabbitMQ (ACK) → `STOCK_DESCONTADO` en Kafka → carrito `disponible false` |
| ✅ | `36_aws_rabbitmq_dashboard.png` | Dashboard RabbitMQ en AWS: 4 colas *running* con DLX/TTL |
| ✅ | `36b_aws_rabbitmq_nodos.png` | RabbitMQ en AWS → *Overview*: los 3 nodos del clúster |
| ✅ | `37_aws_kafkaui_metricas.png` | Kafka-UI en AWS → *Brokers*: 3 brokers, 0 réplicas fuera de sincronía |
| ✅ | `37b_aws_kafkaui_topicos.png` | Kafka-UI en AWS → *Topics*: mensajes por tópico, particiones y RF 3 |
| ✅ | `37c_aws_kafkaui_mensajes_inventario.png` | Kafka-UI en AWS → evento `STOCK_DESCONTADO` (6 → 0) del pedido `PED-EP3-AWS-0001` |
| ✅ | `37d_aws_kafkaui_consumers.png` | Kafka-UI en AWS → *Consumers*: 2 grupos STABLE con 3 miembros |

## 8. Entrega
| ✔ | Archivo | Qué debe verse |
|---|---|---|
| ✅ | `38_repo_publico.png` | Repositorio GitHub público |
