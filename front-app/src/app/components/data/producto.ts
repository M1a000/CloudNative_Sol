import { Producto } from '../models/producto';

export const PRODUCTOS: Producto[] = [

    {
        id: 1,
        nombre: 'Hamburguesa Clásica',
        descripcion: 'Carne de vacuno, queso, lechuga, tomate y salsa especial.',
        precio: 5990,
        imagen: 'https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcScRz3jE-s2KzdkDRyd6WLmt3yqtqu_H2EtaUNeTmNxOw&s=10',
        categoria: 'Hamburguesas',
        stock: 10
    },

    {
        id: 2,
        nombre: 'Hamburguesa BBQ',
        descripcion: 'Carne de vacuno, queso cheddar, tocino y salsa BBQ.',
        precio: 7490,
        imagen: 'https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSYpDh7Hqciw3KJLABCdRMT5xLWLn1PyAIweK9sEO4fzw&s=10',
        categoria: 'Hamburguesas',
        stock: 8
    },

    {
        id: 3,
        nombre: 'Hamburguesa Doble',
        descripcion: 'Doble carne, doble queso, cebolla y salsa especial.',
        precio: 8990,
        imagen: '/img/hamburguesa-doble.jpeg',
        categoria: 'Hamburguesas',
        stock: 6
    },

    {
        id: 4,
        nombre: 'Papas Fritas',
        descripcion: 'Papas fritas crujientes con sal.',
        precio: 2990,
        imagen: '/img/papas-fritas.jpeg',
        categoria: 'Acompañamientos',
        stock: 15
    },

    {
        id: 5,
        nombre: 'Coca-Cola',
        descripcion: 'Coca-Cola de 500ml.',
        precio: 1990,
        imagen: '/img/coca-cola.jpeg',
        categoria: 'Bebidas',
        stock: 20
    }

];