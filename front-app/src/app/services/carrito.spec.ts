import { TestBed } from '@angular/core/testing';
import { HttpTestingController } from '@angular/common/http/testing';
import { PROVEEDORES_TEST } from '../testing/proveedores-test';
import { CarritoService } from './carrito';
import { ItemCarrito } from '../components/models/carrito';

describe('Carrito', () => {
  let service: CarritoService;
  let http: HttpTestingController;

  const itemBackend = (id: number, disponible: boolean | null | undefined, precio = 5990) => ({
    id, usuarioId: 'usuario-anonimo', productoId: 1, nombreProducto: 'Clasica',
    cantidad: 2, precioUnitario: precio, total: precio * 2, disponible,
  });

  beforeEach(() => {
    TestBed.configureTestingModule({ providers: PROVEEDORES_TEST });
    service = TestBed.inject(CarritoService);
    http = TestBed.inject(HttpTestingController);
  });

  afterEach(() => http.verify());

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('marca como sin stock solo los items con disponible=false (sincronizado por Kafka)', () => {
    let items: ItemCarrito[] = [];
    service.obtenerItems().subscribe((r) => (items = r));

    http.expectOne((r) => r.url.endsWith('/api/carrito/usuario-anonimo'))
      .flush([itemBackend(1, true), itemBackend(2, false), itemBackend(3, null), itemBackend(4, undefined)]);

    expect(items.map((i) => i.disponible)).toEqual([true, false, true, true]);
  });

  it('usa el precio vigente que entrega el backend', () => {
    let items: ItemCarrito[] = [];
    service.obtenerItems().subscribe((r) => (items = r));

    http.expectOne((r) => r.url.endsWith('/api/carrito/usuario-anonimo'))
      .flush([itemBackend(1, true, 7990)]);

    expect(items[0].producto.precio).toBe(7990);
  });
});
