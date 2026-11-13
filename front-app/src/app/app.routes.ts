import { Routes } from '@angular/router';
import { MsalGuard } from '@azure/msal-angular';

import { HomeComponent } from './pages/home/home';
import { CatalogoComponent2 } from './pages/catalogo/catalogo2';
import { CarritoComponent } from './pages/carrito/carrito';
import { LoginComponent } from './pages/login/login';
import { MasVendidosComponent } from './pages/mas-vendidos/mas-vendidos';

export const routes: Routes = [

    {
        path: '',
        component: HomeComponent
    },

    {
        path: 'productos',
        component: CatalogoComponent2
    },

    {
        // Protegida: si no hay sesion iniciada, MsalGuard redirige automaticamente al login de Azure AD
        path: 'carrito',
        component: CarritoComponent,
        canActivate: [MsalGuard]
    },

    {
        // Analitica alimentada por Kafka; el endpoint exige JWT, por eso tambien va protegida
        path: 'mas-vendidos',
        component: MasVendidosComponent,
        canActivate: [MsalGuard]
    },

    {
        path: 'login',
        component: LoginComponent
    }

];
