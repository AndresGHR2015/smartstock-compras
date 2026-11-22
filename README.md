# SmartStock · Compras

Microservicio que gestiona proveedores, cotizaciones y órdenes de compra. Recibe alertas de stock bajo y mantiene el estado de la reposición.

**Stack** · C#, .NET 10, PostgreSQL y Kafka.

## Alcance del MVP

- Recibir una alerta de Inventario y dejarla disponible para el flujo de reposición.
- Mantener dos cotizaciones precargadas para el SKU de demostración, con costo, plazo y pedido mínimo.
- Validar y guardar un borrador solicitado por el agente, y registrar la aprobación del operador.

## Integraciones

| Conexión | Protocolo | Operación |
|---|---|---|
| Inventario → Compras | Kafka | Consumir `inventory.stock_low.v1`. |
| Gateway → Compras | Kafka | Solicitar un borrador y su posterior aprobación. |
| Compras → Gateway | Kafka | Informar cambios de estado de la orden. |

## Primera verificación

Procesar una alerta de stock bajo y comprobar que se registra una sola necesidad de reposición aunque Kafka entregue el mismo evento más de una vez.

Las cotizaciones y órdenes se guardan en la base PostgreSQL de Compras. El repo tendrá su Dockerfile, su pipeline y una exigencia de cobertura mínima del 60 %. El despliegue en Kubernetes se integra desde `smartstock-infra`.
