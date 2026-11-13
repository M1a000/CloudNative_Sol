import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, map } from 'rxjs';
import { MsalService } from '@azure/msal-angular';

import { Producto } from '../components/models/producto';
import { ItemCarrito } from '../components/models/carrito';
import { PRODUCTOS } from '../components/data/producto';
import { environment } from '../../environments/environment';

// Forma en la que el backend (venta-carrito) devuelve cada item.
interface ItemCarritoBackend {
    id: number;
    usuarioId: string;
    productoId: number;
    nombreProducto: string;
    cantidad: number;
    precioUnitario: number;
    total: number;
    // Sincronizado desde Kafka (pedidos360.inventario.actualizado). null = disponible.
    disponible?: boolean | null;
}

// Respuesta de POST /api/carrito/{usuarioId}/confirmar
export interface PedidoConfirmado {
    pedidoId: string;
    usuarioId: string;
    total: number;
    items: { productoId: number; nombreProducto: string; cantidad: number; precioUnitario: number }[];
}

@Injectable({
    providedIn: 'root'
})
export class CarritoService {

    private http = inject(HttpClient);
    private msalService = inject(MsalService);

    // Llama al microservicio venta-carrito a traves del API Manager (AWS API Gateway).
    private baseUrl = `${environment.carritoUrl}/api/carrito`;

    // El usuarioId viene de la cuenta autenticada con MSAL (no se inventa en el frontend).
    private obtenerUsuarioId(): string {
        const cuenta = this.msalService.instance.getActiveAccount();
        return cuenta?.localAccountId ?? cuenta?.username ?? 'usuario-anonimo';
    }

    obtenerItems(): Observable<ItemCarrito[]> {
        const usuarioId = this.obtenerUsuarioId();

        return this.http
            .get<ItemCarritoBackend[]>(`${this.baseUrl}/${usuarioId}`)
            .pipe(
                map((items) => items.map((item) => this.aItemCarrito(item)))
            );
    }

    agregarProducto(producto: Producto): Observable<ItemCarrito> {
        const usuarioId = this.obtenerUsuarioId();

        const body = {
            usuarioId,
            productoId: producto.id,
            nombreProducto: producto.nombre,
            cantidad: 1,
            precioUnitario: producto.precio,
        };

        return this.http
            .post<ItemCarritoBackend>(this.baseUrl, body)
            .pipe(
                map((item) => this.aItemCarrito(item, producto))
            );
    }

    actualizarCantidad(backendId: number, cantidad: number): Observable<ItemCarritoBackend> {
        return this.http.put<ItemCarritoBackend>(`${this.baseUrl}/${backendId}`, { cantidad });
    }

    eliminarItem(backendId: number): Observable<void> {
        return this.http.delete<void>(`${this.baseUrl}/${backendId}`);
    }

    // Confirma el carrito como pedido. El backend responde 202 de inmediato y publica el
    // evento "pedido.creado" en RabbitMQ: el stock y el ticket se procesan despues.
    confirmarPedido(): Observable<PedidoConfirmado> {
        const usuarioId = this.obtenerUsuarioId();

        return this.http.post<PedidoConfirmado>(`${this.baseUrl}/${usuarioId}/confirmar`, null);
    }

    // Convierte la respuesta del backend al modelo que usan las paginas/componentes del front.
    // Si tenemos el producto completo a mano (recien agregado), lo usamos para no perder
    // imagen/descripcion/categoria/stock, que el backend de carrito no guarda.
    private aItemCarrito(item: ItemCarritoBackend, productoCompleto?: Producto): ItemCarrito {
        // El backend de carrito no guarda imagen ni descripcion: se toman del catalogo del front.
        const producto = productoCompleto ?? PRODUCTOS.find((p) => p.id === item.productoId) ?? {
                id: item.productoId,
                nombre: item.nombreProducto,
                descripcion: '',
                precio: item.precioUnitario,
                imagen: '',
                categoria: '',
                stock: 0,
            };

        return {
            backendId: item.id,
            cantidad: item.cantidad,
            // El precio vigente es el del backend: Kafka lo actualiza si cambia en el catalogo.
            producto: { ...producto, precio: item.precioUnitario ?? producto.precio },
            disponible: item.disponible !== false,
        };
    }

}
