export const environment = {
  production: false,

  // Datos de la App Registration creada en el Tenant de Azure AD (guias 1.2.3 y 1.2.5)
  azureAd: {
    // Application (client) ID de la App Registration
    clientId: '69ceeda7-0b2f-441c-9c63-f537ab6db86f',
    // Directory (tenant) ID
    tenantId: 'a70c6bf7-a13d-4110-9625-747412057e28',
    redirectUri: 'http://localhost:4200',
    postLogoutRedirectUri: 'http://localhost:4200',
    // Scope expuesto por la API en la App Registration, ej: 'api://<clientId>/access_as_user'
    apiScopes: ['api://69ceeda7-0b2f-441c-9c63-f537ab6db86f/access_as_user'],
  },

  // URL de cada microservicio en desarrollo (ng serve). Para probar contra la EC2,
  // reemplazar localhost por la IP que imprime terraform apply.
  catalogoUrl: 'http://localhost:8081',
  carritoUrl: 'http://localhost:8082',
};
