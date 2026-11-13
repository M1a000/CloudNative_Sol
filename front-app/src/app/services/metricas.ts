import { Injectable, inject } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';

import { MetricaVenta } from '../components/models/metrica-venta';
import { environment } from '../../environments/environment';

@Injectable({
    providedIn: 'root'
})
export class MetricasService {

    private http = inject(HttpClient);

    // Endpoint de analitica de productos-catalogo, alimentado por Kafka.
    // El JWT lo agrega el MsalInterceptor (app.config.ts).
    private baseUrl = `${environment.catalogoUrl}/api/productos/mas-vendidos`;

    obtenerMasVendidos(top = 5): Observable<MetricaVenta[]> {
        const params = new HttpParams().set('top', top);
        return this.http.get<MetricaVenta[]>(this.baseUrl, { params });
    }

}
