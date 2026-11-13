# =============================================================================
# API Manager: AWS API Gateway (HTTP API) delante de los 4 microservicios.
#
# El frontend llama a UNA sola URL (https://<id>.execute-api.us-east-1.amazonaws.com)
# y el gateway enruta segun el prefijo de la ruta:
#
#   /api/productos/**  -> productos-catalogo :8081
#   /api/carrito/**    -> venta-carrito      :8082
#   /api/rabbitmq/**   -> rabbitmq-admin     :8083
#   /api/kafka/**      -> kafka-admin        :8084
#
# Integracion HTTP_PROXY: el gateway reenvia metodo, ruta, query string y cabeceras
# (incluida Authorization con el JWT de Azure AD). La validacion del JWT y el CORS los
# sigue haciendo cada microservicio, igual que cuando se llaman directo.
# =============================================================================

# Rangos de IP publicados por AWS desde los que API Gateway se conecta a los backends.
# El security group abre los puertos de los microservicios SOLO a estos rangos.
data "aws_ip_ranges" "api_gateway" {
  regions  = [var.aws_region]
  services = ["api_gateway"]
}

locals {
  # prefijo de la API -> puerto del microservicio en la EC2
  servicios_api = {
    productos = 8081
    carrito   = 8082
    rabbitmq  = 8083
    kafka     = 8084
  }

  # Dos rutas por servicio: la base (/api/productos) y todo lo que cuelga de ella (/api/productos/...)
  rutas_api = merge(
    { for nombre, puerto in local.servicios_api : "${nombre}-base" => {
      ruta = "ANY /api/${nombre}"
      uri  = "http://${aws_instance.servidor_infraestructura.public_ip}:${puerto}/api/${nombre}"
    } },
    { for nombre, puerto in local.servicios_api : "${nombre}-proxy" => {
      ruta = "ANY /api/${nombre}/{proxy+}"
      uri  = "http://${aws_instance.servidor_infraestructura.public_ip}:${puerto}/api/${nombre}/{proxy}"
    } }
  )
}

resource "aws_apigatewayv2_api" "api" {
  name          = "${var.proyecto}-api"
  protocol_type = "HTTP"
  description   = "API Manager de Pedidos360: enruta el frontend hacia los microservicios de la EC2"

  tags = {
    Name = "${var.proyecto}-api"
  }
}

resource "aws_apigatewayv2_integration" "servicios" {
  for_each = local.rutas_api

  api_id               = aws_apigatewayv2_api.api.id
  integration_type     = "HTTP_PROXY"
  integration_method   = "ANY"
  integration_uri      = each.value.uri
  timeout_milliseconds = 29000
}

resource "aws_apigatewayv2_route" "servicios" {
  for_each = local.rutas_api

  api_id    = aws_apigatewayv2_api.api.id
  route_key = each.value.ruta
  target    = "integrations/${aws_apigatewayv2_integration.servicios[each.key].id}"
}

# Stage $default con despliegue automatico: la URL queda sin sufijo de stage
resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.api.id
  name        = "$default"
  auto_deploy = true

  default_route_settings {
    throttling_burst_limit = 100
    throttling_rate_limit  = 50
  }
}

output "api_gateway_url" {
  description = "URL del API Manager. Va en catalogoUrl y carritoUrl de front-app/src/environments/"
  value       = aws_apigatewayv2_stage.default.invoke_url
}
