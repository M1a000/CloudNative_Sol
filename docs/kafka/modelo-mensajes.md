# Modelo de mensajes Kafka – Pedidos360 (EP3)

Este documento define el formato JSON de **entrada** (lo que publica el productor) y de **salida** (lo que produce el consumidor al procesar) de cada tópico definido en [casos-de-uso-kafka.md](casos-de-uso-kafka.md).
Los DTO Java de las tareas 3.3 y 3.4 se construyen a partir de estas definiciones.

## 1. Convenciones comunes

| Convención | Regla |
|---|---|
| Serialización | Clave (`key`) como **String** con `StringSerializer`. Valor (`value`) como **JSON** con `JsonSerializer` |
| Cabecera de tipo | Se desactiva `__TypeId__` (`spring.json.add.type.headers=false`). Cada consumidor indica su propio DTO destino, así los servicios no comparten paquetes Java |
| Nombres de campo | `camelCase` y en español, igual que el evento `pedido.creado` de RabbitMQ |
| Fechas | ISO-8601 en UTC (`Instant` con `@JsonFormat(shape = STRING)`), ejemplo `"2026-11-11T14:32:05Z"`. El productor puede incluir fracciones de segundo (`"...05.123456Z"`) y ambos formatos se aceptan |
| Montos | Número en **pesos chilenos (CLP)**, sin decimales en la práctica. Se mantiene `Double` por consistencia con el código existente |
| Versión | El campo `version` (entero) permite evolucionar el esquema. Si un consumidor recibe una versión que no conoce, envía el mensaje al DLT |

### Campos de cabecera del evento (presentes en todos los mensajes)

| Campo | Tipo | Obligatorio | Descripción |
|---|---|---|---|
| `eventoId` | String (UUID) | Sí | Identificador único del evento. Se usa para **idempotencia** |
| `tipoEvento` | String (enum) | Sí | Qué ocurrió (ver cada tópico) |
| `version` | Integer | Sí | Versión del esquema. Actualmente `1` |
| `fechaEvento` | String (ISO-8601 UTC) | Sí | Momento en que ocurrió el hecho |
| `origen` | String | Sí | Microservicio que publica (`venta-carrito`, `productos-catalogo`) |

---

## 2. Tópico `pedidos360.ventas.confirmadas`

**Productor:** `venta-carrito` · **Consumidor:** `productos-catalogo` (grupo `catalogo-metricas-ventas`)
**Key:** `usuarioId` · **DTO:** `VentaConfirmadaEvent`

### 2.1 Entrada: mensaje publicado

```json
{
  "eventoId": "7c1f0a52-4b8e-4f0e-9a51-2d6f3c9b8e14",
  "tipoEvento": "VENTA_CONFIRMADA",
  "version": 1,
  "fechaEvento": "2026-11-11T14:32:05Z",
  "origen": "venta-carrito",
  "pedidoId": "b3e2d1c0-9f8e-4d7c-8b6a-5e4f3d2c1b0a",
  "usuarioId": "solange@pedidos360.cl",
  "items": [
    {
      "productoId": 3,
      "nombreProducto": "Audífonos Bluetooth",
      "cantidad": 2,
      "precioUnitario": 24990,
      "subtotal": 49980
    },
    {
      "productoId": 7,
      "nombreProducto": "Mouse inalámbrico",
      "cantidad": 1,
      "precioUnitario": 12990,
      "subtotal": 12990
    }
  ],
  "cantidadUnidades": 3,
  "total": 62970,
  "moneda": "CLP"
}
```

| Campo | Tipo | Validación |
|---|---|---|
| `tipoEvento` | String | Debe ser `VENTA_CONFIRMADA` |
| `pedidoId` | String (UUID) | `@NotBlank`. Es el mismo id que viaja en `pedido.creado` por RabbitMQ, lo que permite trazar el pedido en ambos brokers |
| `usuarioId` | String | `@NotBlank` |
| `items` | Lista | `@NotEmpty`, cada ítem `@Valid` |
| `items[].productoId` | Long | `@NotNull` |
| `items[].nombreProducto` | String | Opcional (informativo) |
| `items[].cantidad` | Integer | `@NotNull @Positive` |
| `items[].precioUnitario` | Double | `@NotNull @PositiveOrZero` |
| `items[].subtotal` | Double | `@NotNull`, igual a `cantidad × precioUnitario` |
| `cantidadUnidades` | Integer | `@Positive`, suma de las cantidades |
| `total` | Double | `@NotNull @PositiveOrZero`, suma de los subtotales |
| `moneda` | String | `"CLP"` |

**Reglas de negocio del consumidor.** Si alguna falla, el mensaje va al DLT sin reintentos:
- `total` debe coincidir con la suma de `subtotal` (tolerancia de ±1 por redondeo).
- Si `version` es distinta de 1, se rechaza como versión no soportada.

### 2.2 Salida: resultado del procesamiento

El consumidor actualiza dos tablas en la BD de `productos-catalogo`:

**`metricas_venta_producto`** (una fila por producto, acumulativa)

| Columna | Tipo | Ejemplo tras el evento |
|---|---|---|
| `producto_id` (PK) | BIGINT | 3 |
| `nombre_producto` | VARCHAR | Audífonos Bluetooth |
| `unidades_vendidas` | INT | +2 |
| `monto_vendido` | DOUBLE | +49980 |
| `cantidad_pedidos` | INT | +1 |
| `ultima_venta` | DATETIME | 2026-11-11 14:32:05 |

**`eventos_procesados`** (idempotencia): `evento_id` (PK), `topico`, `fecha_procesado`. Si el `eventoId` ya existe, el mensaje se confirma sin volver a sumar.

**Endpoint resultante:** `GET /api/productos/mas-vendidos?top=5` (protegido con JWT)

```json
[
  {
    "productoId": 3,
    "nombreProducto": "Audífonos Bluetooth",
    "unidadesVendidas": 14,
    "montoVendido": 349860,
    "cantidadPedidos": 9,
    "ultimaVenta": "2026-11-11T14:32:05Z"
  },
  {
    "productoId": 7,
    "nombreProducto": "Mouse inalámbrico",
    "unidadesVendidas": 11,
    "montoVendido": 142890,
    "cantidadPedidos": 8,
    "ultimaVenta": "2026-11-11T13:10:44Z"
  }
]
```

---

## 3. Tópico `pedidos360.inventario.actualizado`

**Productor:** `productos-catalogo` · **Consumidor:** `venta-carrito` (grupo `carrito-sync-inventario`)
**Key:** `productoId` (como String) · **DTO:** `InventarioActualizadoEvent` · **Política:** `compact`

### 3.1 Entrada: mensaje publicado

Siempre se envía el **estado completo** del producto, no solo lo que cambió. Con compactación, el último mensaje de cada `productoId` debe bastar por sí solo para conocer el producto.

**a) Stock descontado por un pedido** (publicado desde `StockPedidoConsumer` después de procesar `pedido.creado` de RabbitMQ):

```json
{
  "eventoId": "e5a9c3d1-2b7f-4c8e-a1d0-6f3b9e2c7a58",
  "tipoEvento": "STOCK_DESCONTADO",
  "version": 1,
  "fechaEvento": "2026-11-11T14:32:06Z",
  "origen": "productos-catalogo",
  "productoId": 3,
  "nombre": "Audífonos Bluetooth",
  "categoria": "Tecnología",
  "precio": 24990,
  "stockAnterior": 2,
  "stockActual": 0,
  "disponible": false,
  "pedidoId": "b3e2d1c0-9f8e-4d7c-8b6a-5e4f3d2c1b0a"
}
```

**b) Producto creado o actualizado vía API** (`POST` / `PUT /api/productos`), por ejemplo un cambio de precio:

```json
{
  "eventoId": "0d4b8e2a-6c1f-4a9e-b3d7-8e5c2f1a9b60",
  "tipoEvento": "PRODUCTO_ACTUALIZADO",
  "version": 1,
  "fechaEvento": "2026-11-11T15:05:00Z",
  "origen": "productos-catalogo",
  "productoId": 7,
  "nombre": "Mouse inalámbrico",
  "categoria": "Tecnología",
  "precio": 10990,
  "stockAnterior": 25,
  "stockActual": 25,
  "disponible": true,
  "pedidoId": null
}
```

**c) Producto eliminado** (`DELETE /api/productos/{id}`): se publica un **tombstone**, es decir, key `"7"` con valor `null`. La compactación elimina todas las versiones anteriores de ese producto después de `delete.retention.ms` (1 día).

| Campo | Tipo | Validación |
|---|---|---|
| `tipoEvento` | String (enum) | `PRODUCTO_CREADO`, `PRODUCTO_ACTUALIZADO` o `STOCK_DESCONTADO` |
| `productoId` | Long | `@NotNull`, igual a la key del mensaje |
| `nombre` | String | `@NotBlank` |
| `categoria` | String | Opcional |
| `precio` | Double | `@NotNull @PositiveOrZero` |
| `stockAnterior` | Integer | `@PositiveOrZero`. Es `null` en `PRODUCTO_CREADO` |
| `stockActual` | Integer | `@NotNull @PositiveOrZero` |
| `disponible` | Boolean | `@NotNull`, igual a `stockActual > 0` |
| `pedidoId` | String | Obligatorio solo en `STOCK_DESCONTADO`. Permite trazar el pedido de RabbitMQ que causó el cambio |

### 3.2 Salida: resultado del procesamiento

El consumidor actualiza en la BD de `venta-carrito` todos los ítems en carrito con ese `productoId`. Para eso, la entidad `ItemCarrito` suma dos campos:

| Campo | Cambio |
|---|---|
| `precioUnitario` | Toma el valor de `precio` |
| `total` | Se recalcula como `cantidad × precioUnitario` |
| `disponible` *(nuevo)* | Toma el valor de `disponible`. Con un tombstone queda en `false` |
| `actualizadoDesdeInventario` *(nuevo)* | Toma el valor de `fechaEvento` |

**Endpoint resultante:** `GET /api/carrito/{usuarioId}` (ya existe y suma los campos nuevos)

```json
[
  {
    "id": 15,
    "usuarioId": "solange@pedidos360.cl",
    "productoId": 7,
    "nombreProducto": "Mouse inalámbrico",
    "cantidad": 2,
    "precioUnitario": 10990,
    "total": 21980,
    "fechaCreacion": "2026-11-11T12:40:10",
    "disponible": true,
    "actualizadoDesdeInventario": "2026-11-11T15:05:00Z"
  },
  {
    "id": 16,
    "usuarioId": "solange@pedidos360.cl",
    "productoId": 3,
    "nombreProducto": "Audífonos Bluetooth",
    "cantidad": 1,
    "precioUnitario": 24990,
    "total": 24990,
    "fechaCreacion": "2026-11-11T12:41:02",
    "disponible": false,
    "actualizadoDesdeInventario": "2026-11-11T14:32:06Z"
  }
]
```

Si el carrito tiene algún ítem con `disponible = false`, `POST /api/carrito/{usuarioId}/confirmar` responde **409 Conflict** con el nombre de los productos agotados y no publica nada (ni en RabbitMQ ni en Kafka). El frontend muestra esos ítems con el aviso "Sin stock" (tarea 4.1).

Para descartar eventos que lleguen fuera de orden, el consumidor ignora un evento cuya `fechaEvento` es anterior al `actualizadoDesdeInventario` del ítem.

---

## 4. Mensajes en los tópicos DLT

Cuando un mensaje no se puede procesar, `DeadLetterPublishingRecoverer` lo copia a `<tópico>.DLT` **con el mismo key y valor original**, y agrega cabeceras con el diagnóstico:

| Cabecera | Ejemplo |
|---|---|
| `kafka_dlt-original-topic` | `pedidos360.ventas.confirmadas` |
| `kafka_dlt-original-partition` | `1` |
| `kafka_dlt-original-offset` | `42` |
| `kafka_dlt-original-consumer-group` | `catalogo-metricas-ventas` |
| `kafka_dlt-exception-fqcn` | `com.pedidos360.catalogo.messaging.comun.MensajeInvalidoException` |
| `kafka_dlt-exception-message` | `items[0].cantidad: debe ser mayor que 0` |

Ejemplo de mensaje inválido que termina en el DLT, porque la cantidad es negativa y el total no cuadra:

```json
{
  "eventoId": "11111111-2222-3333-4444-555555555555",
  "tipoEvento": "VENTA_CONFIRMADA",
  "version": 1,
  "fechaEvento": "2026-11-11T16:00:00Z",
  "origen": "venta-carrito",
  "pedidoId": "pedido-prueba-dlt",
  "usuarioId": "solange@pedidos360.cl",
  "items": [{ "productoId": 3, "cantidad": -1, "precioUnitario": 24990, "subtotal": -24990 }],
  "cantidadUnidades": -1,
  "total": 0,
  "moneda": "CLP"
}
```

Este ejemplo sirve como **prueba de manejo de errores** en el documento de evidencias. Se publica desde Kafka-UI y luego se verifica en el log del consumidor y en el tópico `.DLT`.

---

## 5. DTO Java (referencia para las tareas 3.3 y 3.4)

```java
// venta-carrito (productor) y productos-catalogo (consumidor): una copia en cada servicio
public record VentaConfirmadaEvent(
        @NotBlank String eventoId,
        @NotBlank String tipoEvento,
        @NotNull Integer version,
        @NotNull Instant fechaEvento,
        @NotBlank String origen,
        @NotBlank String pedidoId,
        @NotBlank String usuarioId,
        @NotEmpty List<@Valid @NotNull Item> items,
        @Positive Integer cantidadUnidades,
        @NotNull @PositiveOrZero Double total,
        @NotBlank String moneda) {

    public record Item(
            @NotNull Long productoId,
            String nombreProducto,
            @NotNull @Positive Integer cantidad,
            @NotNull @PositiveOrZero Double precioUnitario,
            @NotNull Double subtotal) {
    }
}

// productos-catalogo (productor) y venta-carrito (consumidor)
public record InventarioActualizadoEvent(
        @NotBlank String eventoId,
        @NotNull TipoEventoInventario tipoEvento,
        @NotNull Integer version,
        @NotNull Instant fechaEvento,
        @NotBlank String origen,
        @NotNull Long productoId,
        @NotBlank String nombre,
        String categoria,
        @NotNull @PositiveOrZero Double precio,
        @PositiveOrZero Integer stockAnterior,
        @NotNull @PositiveOrZero Integer stockActual,
        @NotNull Boolean disponible,
        String pedidoId) {
}

public enum TipoEventoInventario { PRODUCTO_CREADO, PRODUCTO_ACTUALIZADO, STOCK_DESCONTADO }
```
