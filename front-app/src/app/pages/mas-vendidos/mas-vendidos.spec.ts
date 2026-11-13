import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpTestingController } from '@angular/common/http/testing';

import { PROVEEDORES_TEST } from '../../testing/proveedores-test';
import { MasVendidosComponent } from './mas-vendidos';
import { MetricaVenta } from '../../components/models/metrica-venta';

describe('MasVendidos', () => {
  let component: MasVendidosComponent;
  let fixture: ComponentFixture<MasVendidosComponent>;
  let http: HttpTestingController;

  const ranking: MetricaVenta[] = [
    { productoId: 1, nombreProducto: 'Clasica', unidadesVendidas: 10, montoVendido: 59900,
      cantidadPedidos: 4, ultimaVenta: '2026-11-12T10:00:00Z' },
    { productoId: 2, nombreProducto: 'Doble', unidadesVendidas: 5, montoVendido: 39950,
      cantidadPedidos: 2, ultimaVenta: null },
  ];

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [MasVendidosComponent],
      providers: PROVEEDORES_TEST,
    }).compileComponents();

    fixture = TestBed.createComponent(MasVendidosComponent);
    component = fixture.componentInstance;
    http = TestBed.inject(HttpTestingController);
    fixture.detectChanges();
  });

  afterEach(() => http.verify());

  it('muestra el ranking recibido del backend', () => {
    http.expectOne((r) => r.url.endsWith('/mas-vendidos')).flush(ranking);
    fixture.detectChanges();

    const filas = fixture.nativeElement.querySelectorAll('.fila');
    expect(filas.length).toBe(2);
    expect(filas[0].textContent).toContain('Clasica');
    expect(component.porcentaje(ranking[1])).toBe(50);
  });

  it('muestra un mensaje cuando aun no hay ventas', () => {
    http.expectOne((r) => r.url.endsWith('/mas-vendidos')).flush([]);
    fixture.detectChanges();

    expect(fixture.nativeElement.textContent).toContain('Aún no hay ventas registradas');
  });

  it('muestra un error si el backend falla', () => {
    http.expectOne((r) => r.url.endsWith('/mas-vendidos'))
      .flush('error', { status: 500, statusText: 'Server Error' });
    fixture.detectChanges();

    expect(component.error()).not.toBe('');
    expect(fixture.nativeElement.querySelector('.error')).toBeTruthy();
  });

  it('vuelve a consultar al cambiar el top', () => {
    http.expectOne((r) => r.url.endsWith('/mas-vendidos')).flush(ranking);

    component.cambiarTop('10');

    const req = http.expectOne((r) => r.url.endsWith('/mas-vendidos'));
    expect(req.request.params.get('top')).toBe('10');
    req.flush(ranking);
  });
});
