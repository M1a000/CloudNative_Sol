import { Component, Input, inject, signal } from '@angular/core';
import { DecimalPipe } from '@angular/common';
import { RouterLink } from '@angular/router';

import { Producto } from '../../components/models/producto';
import { CarritoService } from '../../services/carrito';
import { PanelCarritoService } from '../../services/panel-carrito';

@Component({
    selector: 'app-producto-card',
    imports: [DecimalPipe, RouterLink],
    templateUrl: './producto-card.html',
    styleUrl: './producto-card.css'
})
export class ProductCardComponent {

    @Input() producto!: Producto;

    private carritoService = inject(CarritoService);
    private panel = inject(PanelCarritoService);

    agregando = signal(false);
    error = signal('');
    requiereLogin = signal(false);

    agregarAlCarrito(): void {

        this.agregando.set(true);
        this.error.set('');
        this.requiereLogin.set(false);

        this.carritoService.agregarProducto(this.producto).subscribe({
            next: () => {
                this.agregando.set(false);
                // En vez de un alert, se abre el panel lateral con el carrito actualizado
                this.panel.productoAgregado(this.producto.nombre);
            },
            error: (err) => {
                console.error('Error al agregar al carrito', err);
                this.agregando.set(false);
                const sinSesion = !this.carritoService.haySesion() || err?.status === 401;
                this.requiereLogin.set(sinSesion);
                this.error.set(sinSesion
                    ? 'Para agregar productos primero debes'
                    : 'No se pudo agregar el producto. Intenta de nuevo.');
            }
        });

    }

}
