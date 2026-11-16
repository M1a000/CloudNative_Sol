import { Injectable, computed, inject, signal } from '@angular/core';
import { Observable } from 'rxjs';

import { CarritoService } from './carrito';
import { ItemCarrito } from '../components/models/carrito';

/**
 * Estado compartido del carrito para el boton de la barra y el panel lateral
 * que aparece al agregar un producto (reemplaza al antiguo alert()).
 * Los datos siempre vienen del backend venta-carrito: aqui solo se guardan para la vista.
 */
@Injectable({
    providedIn: 'root'
})
export class PanelCarritoService {

    private carritoService = inject(CarritoService);

    items = signal<ItemCarrito[]>([]);
    abierto = signal(false);
    mensaje = signal('');

    // Los productos sin stock (marcados por Kafka) no suman al total
    total = computed(() => this.items()
        .filter((item) => item.disponible !== false)
        .reduce((suma, item) => suma + item.producto.precio * item.cantidad, 0));

    unidades = computed(() => this.items().reduce((suma, item) => suma + item.cantidad, 0));

    refrescar(): void {
        this.carritoService.obtenerItems().subscribe({
            next: (items) => this.items.set(items),
            error: (err) => console.error('Error al obtener el carrito', err)
        });
    }

    productoAgregado(nombre: string): void {
        this.mensaje.set(`${nombre} agregado`);
        this.refrescar();
        this.abierto.set(true);
    }

    abrir(): void {
        this.mensaje.set('');
        this.refrescar();
        this.abierto.set(true);
    }

    cerrar(): void {
        this.abierto.set(false);
    }

    cambiarCantidad(item: ItemCarrito, delta: number): void {
        if (!item.backendId) return;

        const nueva = item.cantidad + delta;
        const peticion: Observable<unknown> = nueva <= 0
            ? this.carritoService.eliminarItem(item.backendId)
            : this.carritoService.actualizarCantidad(item.backendId, nueva);

        peticion.subscribe({
            next: () => this.refrescar(),
            error: (err) => console.error('Error al actualizar el carrito', err)
        });
    }
}
