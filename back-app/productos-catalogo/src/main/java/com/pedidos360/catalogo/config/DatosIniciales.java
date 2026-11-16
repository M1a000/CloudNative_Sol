package com.pedidos360.catalogo.config;

import com.pedidos360.catalogo.model.Producto;
import com.pedidos360.catalogo.repository.ProductoRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Carga el catalogo inicial cuando la tabla de productos esta vacia (base de datos nueva).
 * Se insertan en este orden para que queden con los ids 1 a 5, los mismos que usa el frontend.
 */
@Component
public class DatosIniciales implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(DatosIniciales.class);

    private final ProductoRepository productoRepository;

    public DatosIniciales(ProductoRepository productoRepository) {
        this.productoRepository = productoRepository;
    }

    @Override
    public void run(String... args) {
        if (productoRepository.count() > 0) {
            return;
        }
        List<Producto> productos = List.of(
                new Producto("Hamburguesa Clásica", "Carne de vacuno, queso, lechuga, tomate y salsa especial.", 5990.0, 10, "Hamburguesas"),
                new Producto("Hamburguesa BBQ", "Carne de vacuno, queso cheddar, tocino y salsa BBQ.", 7490.0, 8, "Hamburguesas"),
                new Producto("Hamburguesa Doble", "Doble carne, doble queso, cebolla y salsa especial.", 8990.0, 6, "Hamburguesas"),
                new Producto("Papas Fritas", "Papas fritas crujientes con sal.", 2990.0, 15, "Acompañamientos"),
                new Producto("Coca-Cola", "Coca-Cola de 500ml.", 1990.0, 20, "Bebidas"));
        // Uno por uno para respetar el orden de los ids autogenerados
        productos.forEach(productoRepository::save);
        log.info("[CATALOGO] Tabla vacia: se cargaron {} productos iniciales", productos.size());
    }
}
