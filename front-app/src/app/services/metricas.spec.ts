import { TestBed } from '@angular/core/testing';
import { HttpTestingController } from '@angular/common/http/testing';

import { PROVEEDORES_TEST } from '../testing/proveedores-test';
import { MetricasService } from './metricas';
import { MetricaVenta } from '../components/models/metrica-venta';
import { environment } from '../../environments/environment';

describe('MetricasService', () => {
  let service: MetricasService;
  let http: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({ providers: PROVEEDORES_TEST });
    service = TestBed.inject(MetricasService);
    http = TestBed.inject(HttpTestingController);
  });

  afterEach(() => http.verify());

  it('pide el ranking al endpoint mas-vendidos con el parametro top', () => {
    const ranking: MetricaVenta[] = [
      { productoId: 1, nombreProducto: 'Clasica', unidadesVendidas: 7, montoVendido: 41930,
        cantidadPedidos: 3, ultimaVenta: '2026-11-12T10:00:00Z' },
    ];
    let recibido: MetricaVenta[] = [];

    service.obtenerMasVendidos(3).subscribe((r) => (recibido = r));

    const req = http.expectOne(
      (r) => r.url === `${environment.catalogoUrl}/api/productos/mas-vendidos`);
    expect(req.request.method).toBe('GET');
    expect(req.request.params.get('top')).toBe('3');
    req.flush(ranking);

    expect(recibido).toEqual(ranking);
  });

  it('usa top=5 por defecto', () => {
    service.obtenerMasVendidos().subscribe();
    const req = http.expectOne((r) => r.url.endsWith('/mas-vendidos'));
    expect(req.request.params.get('top')).toBe('5');
    req.flush([]);
  });
});
