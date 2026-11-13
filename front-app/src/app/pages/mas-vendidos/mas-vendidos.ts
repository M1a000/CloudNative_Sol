import { Component, OnInit, computed, inject, signal } from '@angular/core';
import { DatePipe, DecimalPipe } from '@angular/common';

import { MetricasService } from '../../services/metricas';
import { MetricaVenta } from '../../components/models/metrica-venta';
import { PRODUCTOS } from '../../components/data/producto';

@Component({
    selector: 'app-mas-vendidos',
    imports: [DecimalPipe, DatePipe],
    templateUrl: './mas-vendidos.html',
    styleUrl: './mas-vendidos.css'
})
export class MasVendidosComponent implements OnInit {

    private metricasService = inject(MetricasService);

    // Opciones del selector "Top N"
    readonly opcionesTop = [3, 5, 10];

    top = signal(5);
    ranking = signal<MetricaVenta[]>([]);
    cargando = signal(true);
    error = signal('');

    // Para dibujar la barra proporcional al producto mas vendido
    maxUnidades = computed(() =>
        Math.max(1, ...this.ranking().map((m) => m.unidadesVendidas)));

    ngOnInit(): void {
        this.cargar();
    }

    cambiarTop(valor: string): void {
        this.top.set(Number(valor));
        this.cargar();
    }

    cargar(): void {
        this.cargando.set(true);
        this.error.set('');

        this.metricasService.obtenerMasVendidos(this.top()).subscribe({
            next: (ranking) => {
                this.ranking.set(ranking);
                this.cargando.set(false);
            },
            error: (err) => {
                console.error('Error al obtener los mas vendidos', err);
                this.error.set('No se pudieron cargar las métricas de venta. Intenta de nuevo.');
                this.cargando.set(false);
            }
        });
    }

    // La imagen no viene del backend: se toma del catalogo local del front.
    imagenDe(productoId: number): string {
        return PRODUCTOS.find((p) => p.id === productoId)?.imagen ?? '';
    }

    porcentaje(m: MetricaVenta): number {
        return Math.round((m.unidadesVendidas / this.maxUnidades()) * 100);
    }

}
