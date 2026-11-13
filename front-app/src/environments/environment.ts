export const environment = {
  production: true,

  // Datos de la App Registration creada en el Tenant de Azure AD (guias 1.2.3 y 1.2.5)
  azureAd: {
    clientId: '69ceeda7-0b2f-441c-9c63-f537ab6db86f',
    tenantId: 'a70c6bf7-a13d-4110-9625-747412057e28',
    redirectUri: 'http://localhost:4200',
    postLogoutRedirectUri: 'http://localhost:4200',
    apiScopes: ['api://69ceeda7-0b2f-441c-9c63-f537ab6db86f/access_as_user'],
  },

  // URL del API Manager propio (terraform output api_gateway_url): ambas son la misma porque
  // el gateway enruta /api/productos y /api/carrito. Apuntando directo
  // a la EC2 son http://IP:8081 (catalogo) y http://IP:8082 (carrito).
  catalogoUrl: 'https://3191rd3t14.execute-api.us-east-1.amazonaws.com',
  carritoUrl: 'https://3191rd3t14.execute-api.us-east-1.amazonaws.com',
};
