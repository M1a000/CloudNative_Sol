// Metricas de venta por producto que calcula productos-catalogo al consumir
// el topico Kafka pedidos360.ventas.confirmadas (GET /api/productos/mas-vendidos).
export interface MetricaVenta {
    productoId: number;
    nombreProducto: string;
    unidadesVendidas: number;
    montoVendido: number;
    cantidadPedidos: number;
    ultimaVenta: string | null;
}
