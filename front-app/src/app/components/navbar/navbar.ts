import { Component, OnInit, inject } from '@angular/core';
import { DecimalPipe } from '@angular/common';
import { RouterLink, RouterLinkActive } from '@angular/router';
import { MsalBroadcastService, MsalService } from '@azure/msal-angular';
import { InteractionStatus } from '@azure/msal-browser';
import { filter } from 'rxjs/operators';

import { PanelCarritoService } from '../../services/panel-carrito';

@Component({
  selector: 'app-navbar',
  imports: [
    RouterLink,
    RouterLinkActive,
    DecimalPipe
  ],
  templateUrl: './navbar.html',
  styleUrl: './navbar.css'
})
export class NavbarComponent implements OnInit {

  private msalService = inject(MsalService);
  private msalBroadcastService = inject(MsalBroadcastService);
  panel = inject(PanelCarritoService);

  nombreUsuario: string | null = null;

  ngOnInit(): void {
    this.msalBroadcastService.inProgress$
      .pipe(filter((status) => status === InteractionStatus.None))
      .subscribe(() => {
        const cuenta = this.msalService.instance.getActiveAccount();
        this.nombreUsuario = cuenta?.name ?? cuenta?.username ?? null;
        // Con sesion iniciada, carga el carrito para mostrar total y cantidad en el boton
        if (cuenta) {
          this.panel.refrescar();
        }
      });
  }

  cerrarSesion(): void {
    this.msalService.logoutRedirect();
  }
}
