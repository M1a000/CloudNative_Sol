import { Component, OnDestroy, OnInit, inject, signal } from '@angular/core';

import { Producto } from '../../components/models/producto';
import { PRODUCTOS } from '../../components/data/producto';
import { ProductoService } from '../../services/producto';
import { ProductCardComponent } from '../../components/producto-card/producto-card';

// Cada cuanto se vuelve a consultar el catalogo para reflejar el stock real.
const REFRESCO_MS = 5000;

@Component({
    selector: 'app-catalogo',
    imports: [ ProductCardComponent ],
    templateUrl: './catalogo.html',
    styleUrl: './catalogo.css'
})
export class CatalogoComponent2 implements OnInit, OnDestroy {

    private productoService = inject(ProductoService);
    private temporizador?: ReturnType<typeof setInterval>;

    // Mientras llega la respuesta del backend se muestra la lista local.
    productos = signal<Producto[]>(PRODUCTOS);

    ngOnInit(): void {
        this.cargar();
        this.temporizador = setInterval(() => this.cargar(), REFRESCO_MS);
    }

    ngOnDestroy(): void {
        clearInterval(this.temporizador);
    }

    // Stock, precio y datos vienen de productos-catalogo (via API Gateway);
    // la imagen se toma de la lista local porque el backend no la guarda.
    private cargar(): void {
        this.productoService.obtenerProductos().subscribe({
            next: (productos) => this.productos.set(productos.map((p) => ({
                ...p,
                imagen: p.imagen || PRODUCTOS.find((local) => local.id === p.id)?.imagen || ''
            }))),
            error: (err) => console.error('Error al obtener productos desde el API Gateway', err)
        });
    }

}
