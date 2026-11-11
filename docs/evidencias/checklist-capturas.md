# Checklist de capturas – PDF de evidencias EP3

Guardar cada captura en `docs/evidencias/capturas/` con **el nombre exacto** de la tabla. Claude avisa en el momento de sacar cada una y la marca con ✅ cuando la revisa.

> ⚠️ Antes de guardar, tapar o recortar: tokens JWT completos, claves de AWS, contraseñas y el `tenant-id` / `client-id` si se ven enteros.
> La carpeta `capturas/` está en `.gitignore`: las imágenes no se suben a GitHub, solo van en el PDF.

## 1. Planificación
| ✔ | Archivo | Qué debe verse |
|---|---|---|
| ✅ | *(ya generada)* `docs/planificacion/carta_gantt_EP3_*.png` | Carta Gantt con avance |

## 2. Infraestructura Docker Compose
| ✔ | Archivo | Qué debe verse | Indicador |
|---|---|---|---|
| ⬜ | `01_docker_ps.png` | `docker ps` con los 3 nodos RabbitMQ, Zookeeper, 3 brokers Kafka y Kafka-UI en estado *Up/healthy* | Requisitos docker-compose |
| ⬜ | `02_rabbitmq_cluster_nodos.png` | Dashboard RabbitMQ → *Overview* con los 3 nodos del clúster | Clúster RabbitMQ |
| ⬜ | `03_kafkaui_brokers.png` | Kafka-UI → *Brokers* con los 3 brokers activos | Config. cliente Kafka |
| ⬜ | `04_kafkaui_topicos.png` | Kafka-UI → *Topics* con los 2 tópicos y sus DLT | Parámetros de tópicos |
| ⬜ | `05_kafkaui_config_ventas.png` | Configuración de `pedidos360.ventas.confirmadas`: 3 particiones, RF 3, `retention.ms`, `cleanup.policy=delete` | Parámetros de tópicos |
| ⬜ | `06_kafkaui_config_inventario.png` | Configuración de `pedidos360.inventario.actualizado`: `cleanup.policy=compact` | Parámetros de tópicos |

## 3. Microservicios Kafka
| ✔ | Archivo | Qué debe verse | Indicador |
|---|---|---|---|
| ⬜ | `07_log_productor_venta.png` | Log de `venta-carrito`: evento publicado (tópico, partición, offset) | Productores |
| ⬜ | `08_kafkaui_mensaje_venta.png` | Kafka-UI → *Messages*: el JSON de `VentaConfirmadaEvent` con su key | Productores con DTO |
| ⬜ | `09_log_consumidor_metricas.png` | Log de `productos-catalogo` procesando la venta | Consumidores |
| ⬜ | `10_postman_mas_vendidos.png` | `GET /api/productos/mas-vendidos` con respuesta 200 | Consumidores / flujo de negocio |
| ⬜ | `11_log_productor_inventario.png` | Log de `productos-catalogo` publicando `STOCK_DESCONTADO` | Productores |
| ⬜ | `12_log_consumidor_carrito.png` | Log de `venta-carrito` actualizando el carrito | Consumidores |
| ⬜ | `13_kafkaui_consumer_groups.png` | Kafka-UI → *Consumers*: los 2 grupos, miembros por partición y lag 0 | Consumer groups |
| ⬜ | `14_rabbitmq_sigue_funcionando.png` | Log o dashboard: `pedido.creado` por RabbitMQ sigue procesándose (stock y ticket) | Sin afectar RabbitMQ |

## 4. Manejo de errores
| ✔ | Archivo | Qué debe verse | Indicador |
|---|---|---|---|
| ⬜ | `15_log_reintentos.png` | Log con los reintentos con backoff (1 s, 2 s, 4 s) | Manejo de errores |
| ⬜ | `16_log_mensaje_invalido.png` | Log al recibir un JSON inválido, con la causa y el envío al DLT | Manejo de errores |
| ⬜ | `17_kafkaui_dlt.png` | Kafka-UI → mensaje en `.DLT` con las cabeceras `kafka_dlt-*` | Manejo de errores |

## 5. Microservicio kafka-admin
| ✔ | Archivo | Qué debe verse | Indicador |
|---|---|---|---|
| ⬜ | `18_admin_crear_topico.png` | Postman/Swagger: `POST /api/kafka/topicos` → 201 | Administrador Kafka |
| ⬜ | `19_admin_listar_topicos.png` | `GET /api/kafka/topicos` | Administrador Kafka |
| ⬜ | `20_admin_modificar_config.png` | `PUT …/configuraciones` (por ejemplo, cambiar `retention.ms`), con el cambio visible en Kafka-UI | Administrador Kafka |
| ⬜ | `21_admin_particiones.png` | `PATCH …/particiones` | Administrador Kafka |
| ⬜ | `22_admin_grupos.png` | `GET /api/kafka/grupos/{id}` con offsets y lag | Admin. consumer groups |
| ⬜ | `23_admin_eliminar_topico.png` | `DELETE /api/kafka/topicos/{nombre}` → 204 | Administrador Kafka |
| ⬜ | `24_admin_sin_token_401.png` | Petición sin JWT → 401 | Seguridad JWT |

## 6. Pruebas unitarias
| ✔ | Archivo | Qué debe verse |
|---|---|---|
| ⬜ | `25_tests_venta_carrito.png` | `mvn test` → BUILD SUCCESS con el resumen de tests |
| ⬜ | `26_tests_productos_catalogo.png` | Ídem |
| ⬜ | `27_tests_kafka_admin.png` | Ídem |
| ⬜ | `28_tests_frontend.png` | `ng test` / `npm test` en verde |

## 7. Despliegue AWS y pruebas de integración
| ✔ | Archivo | Qué debe verse |
|---|---|---|
| ⬜ | `29_aws_ec2.png` | Consola AWS: instancia EC2 *running* |
| ⬜ | `30_aws_docker_ps.png` | `docker ps` dentro de la EC2 |
| ⬜ | `31_login_idaas.png` | Pantalla de login de Azure AD desde el frontend desplegado |
| ⬜ | `32_front_logueado.png` | Frontend con la sesión iniciada |
| ⬜ | `33_devtools_bearer.png` | DevTools → *Network*: petición al backend con `Authorization: Bearer …` (token recortado) |
| ⬜ | `34_front_mas_vendidos.png` | Vista "Más vendidos" con datos |
| ⬜ | `35_front_sin_stock.png` | Carrito con el aviso "Sin stock" |
| ⬜ | `36_aws_rabbitmq_dashboard.png` | Dashboard RabbitMQ en AWS: colas con mensajes procesados |
| ⬜ | `37_aws_kafkaui_metricas.png` | Kafka-UI en AWS: tópicos, mensajes y métricas |

## 8. Entrega
| ✔ | Archivo | Qué debe verse |
|---|---|---|
| ⬜ | `38_repo_publico.png` | Repositorio GitHub público |
