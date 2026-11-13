import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpTestingController } from '@angular/common/http/testing';
import { PROVEEDORES_TEST } from '../../testing/proveedores-test';
import { CarritoComponent } from './carrito';

describe('Carrito', () => {
  let component: CarritoComponent;
  let fixture: ComponentFixture<CarritoComponent>;
  let http: HttpTestingController;

  const item = (id: number, disponible: boolean) => ({
    id, usuarioId: 'usuario-anonimo', productoId: id, nombreProducto: 'Producto ' + id,
    cantidad: 1, precioUnitario: 1000, total: 1000, disponible,
  });

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [CarritoComponent],
      providers: PROVEEDORES_TEST,
    }).compileComponents();

    fixture = TestBed.createComponent(CarritoComponent);
    component = fixture.componentInstance;
    http = TestBed.inject(HttpTestingController);
    fixture.detectChanges();
  });

  afterEach(() => http.verify());

  const responderCarrito = (items: unknown[]) => {
    http.expectOne((r) => r.url.endsWith('/api/carrito/usuario-anonimo')).flush(items);
    fixture.detectChanges();
  };

  it('should create', () => {
    responderCarrito([]);
    expect(component).toBeTruthy();
  });

  it('muestra el aviso y la etiqueta "Sin stock" y bloquea la compra', () => {
    responderCarrito([item(1, true), item(2, false)]);

    const html: HTMLElement = fixture.nativeElement;
    expect(html.querySelector('.aviso-sin-stock')).toBeTruthy();
    expect(html.querySelectorAll('.badge-sin-stock').length).toBe(1);
    expect((html.querySelector('.comprar') as HTMLButtonElement).disabled).toBe(true);
    // El total solo considera los productos disponibles
    expect(component.obtenerTotal()).toBe(1000);
  });

  it('no muestra el aviso si todo esta disponible', () => {
    responderCarrito([item(1, true)]);

    const html: HTMLElement = fixture.nativeElement;
    expect(html.querySelector('.aviso-sin-stock')).toBeNull();
    expect((html.querySelector('.comprar') as HTMLButtonElement).disabled).toBe(false);
  });

  it('si el backend responde 409 (sin stock) informa y recarga el carrito', () => {
    responderCarrito([item(1, true)]);

    component.confirmar();
    http.expectOne((r) => r.url.endsWith('/confirmar'))
      .flush({ mensaje: 'sin stock' }, { status: 409, statusText: 'Conflict' });

    expect(component.errorPedido()).toContain('sin stock');
    responderCarrito([item(1, false)]);
    expect(component.itemsSinStock().length).toBe(1);
  });
});
