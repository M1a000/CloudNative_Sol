import { Component, OnInit, inject } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { MsalService } from '@azure/msal-angular';

import { NavbarComponent } from './components/navbar/navbar';
import { FooterComponent } from './components/footer/footer';
import { PanelCarritoComponent } from './components/panel-carrito/panel-carrito';

@Component({
  selector: 'app-root',
  imports: [
    RouterOutlet,
    NavbarComponent,
    FooterComponent,
    PanelCarritoComponent
  ],
  templateUrl: './app.html',
  styleUrl: './app.css'
})
export class AppComponent implements OnInit {

  private msalService = inject(MsalService);

  ngOnInit(): void {
    // Procesa la respuesta de Azure AD cuando el usuario vuelve del login (loginRedirect),
    // y marca la cuenta como activa para que el resto de la app (guards, interceptor,
    // navbar) sepa que hay una sesion iniciada.
    this.msalService.handleRedirectObservable().subscribe({
      next: (result) => {
        if (result?.account) {
          this.msalService.instance.setActiveAccount(result.account);
        } else if (!this.msalService.instance.getActiveAccount()) {
          const accounts = this.msalService.instance.getAllAccounts();
          if (accounts.length > 0) {
            this.msalService.instance.setActiveAccount(accounts[0]);
          }
        }
      },
      error: (error) => {
        console.error('Error procesando el redirect de MSAL:', error);
      }
    });
  }

}