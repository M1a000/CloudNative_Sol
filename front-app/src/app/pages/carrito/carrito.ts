import { Component, OnInit, computed, signal } from '@angular/core';
import { DecimalPipe } from '@angular/common';

import { CarritoService, PedidoConfirmado } from '../../services/carrito';
import { ItemCarrito } from '../../components/models/carrito';

@Component({
    selector: 'app-carrito',
    imports: [DecimalPipe],
    templateUrl: './carrito.html',
    styleUrl: './carrito.css'
})
export class CarritoComponent implements OnInit {

    // Signals: la vista se actualiza apenas cambia el valor, tambien cuando la respuesta
    // HTTP llega fuera de la zona de Angular (pasa con el interceptor de MSAL).
    items = signal<ItemCarrito[]>([]);
    cargando = signal(true);

    confirmando = signal(false);
    pedidoConfirmado = signal<PedidoConfirmado | null>(null);
    errorPedido = signal('');

    // Items agotados segun el ultimo evento de inventario recibido por Kafka
    itemsSinStock = computed(() => this.items().filter((item) => item.disponible === false));

    constructor(
        private carritoService: CarritoService
    ) {}

    ngOnInit(): void {

        this.cargarCarrito();

    }

    aumentar(item: ItemCarrito): void {

        if (!item.backendId) return;

        this.carritoService
            .actualizarCantidad(item.backendId, item.cantidad + 1)
            .subscribe(() => this.cargarCarrito());

    }

    disminuir(item: ItemCarrito): void {

        if (!item.backendId) return;

        if (item.cantidad <= 1) {
            this.eliminar(item);
            return;
        }

        this.carritoService
            .actualizarCantidad(item.backendId, item.cantidad - 1)
            .subscribe(() => this.cargarCarrito());

    }

    eliminar(item: ItemCarrito): void {

        if (!item.backendId) return;

        this.carritoService
            .eliminarItem(item.backendId)
            .subscribe(() => this.cargarCarrito());

    }

    vaciar(): void {

        this.items()
            .filter((item) => item.backendId)
            .forEach((item) => {
                this.carritoService.eliminarItem(item.backendId!).subscribe();
            });

        // Pequeña espera para que las eliminaciones lleguen al backend antes de refrescar
        setTimeout(() => this.cargarCarrito(), 400);

    }

    confirmar(): void {

        this.confirmando.set(true);
        this.errorPedido.set('');

        this.carritoService.confirmarPedido().subscribe({
            next: (pedido) => {
                this.pedidoConfirmado.set(pedido);
                this.items.set([]);
                this.confirmando.set(false);
            },
            error: (err) => {
                console.error('Error al confirmar el pedido', err);
                if (err.status === 409) {
                    // El backend rechaza el pedido si hay productos agotados: se recarga para marcarlos.
                    this.errorPedido.set('Hay productos sin stock en tu carrito. Quítalos para continuar.');
                    this.cargarCarrito();
                } else if (err.status === 422) {
                    this.errorPedido.set('Tu carrito está vacío.');
                } else if (err.status === 503) {
                    this.errorPedido.set('El servicio de pedidos no está disponible. Tu carrito sigue guardado, intenta de nuevo en unos minutos.');
                } else {
                    this.errorPedido.set('No se pudo confirmar el pedido. Intenta de nuevo.');
                }
                this.confirmando.set(false);
            }
        });

    }

    obtenerTotal(): number {

        // Los productos sin stock no se cobran: el pedido no se puede confirmar con ellos
        return this.items().filter((item) => item.disponible !== false).reduce(
            (total, item) => total + (item.producto.precio * item.cantidad),
            0
        );

    }

    private cargarCarrito(): void {

        this.cargando.set(true);

        this.carritoService.obtenerItems().subscribe({
            next: (items) => {
                this.items.set(items);
                this.cargando.set(false);
            },
            error: (err) => {
                console.error('Error al obtener el carrito desde el API Gateway', err);
                this.cargando.set(false);
            }
        });

    }

}
