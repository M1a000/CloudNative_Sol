import { Producto } from './producto';

export interface ItemCarrito {

    producto: Producto;

    cantidad: number;

    // Id de la fila en el backend (venta-carrito). Se usa para actualizar/eliminar el item.
    backendId?: number;

    // false si el producto se agoto o se elimino del catalogo. Lo mantiene venta-carrito
    // consumiendo el topico Kafka pedidos360.inventario.actualizado.
    disponible?: boolean;

}

export interface Carrito {

    items: ItemCarrito[];

    total: number;

}
