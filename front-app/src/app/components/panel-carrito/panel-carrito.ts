import { Component, inject } from '@angular/core';
import { DecimalPipe } from '@angular/common';
import { Router } from '@angular/router';

import { PanelCarritoService } from '../../services/panel-carrito';

@Component({
    selector: 'app-panel-carrito',
    imports: [DecimalPipe],
    templateUrl: './panel-carrito.html',
    styleUrl: './panel-carrito.css'
})
export class PanelCarritoComponent {

    panel = inject(PanelCarritoService);
    private router = inject(Router);

    verCarrito(): void {
        this.panel.cerrar();
        this.router.navigate(['/carrito']);
    }
}
