# SmartStock · Compras

Microservicio que administra el abastecimiento de SmartStock. Mantiene proveedores, cotizaciones y órdenes de compra, y registra las decisiones de reposición desde la alerta de inventario hasta la aprobación del operador.

**Stack** · C#, .NET 10, PostgreSQL y Kafka.

## Responsabilidades

- Recibir alertas de stock bajo y registrar la necesidad de reposición del SKU.
- Mantener la información de proveedores y cotizaciones, incluidos costo, plazo de entrega y cantidad mínima.
- Validar solicitudes de borrador y guardar las órdenes con su proveedor, cantidad y estado.
- Registrar la aprobación o el rechazo del operador y comunicar los cambios de estado al Gateway.

## Datos y reglas de negocio

La base PostgreSQL de Compras es la fuente de verdad para proveedores, cotizaciones y órdenes. El agente puede solicitar un borrador. Compras comprueba que la propuesta cumple las condiciones de la cotización y solo una acción del operador puede aprobarla. La recepción repetida de un evento debe conservar el mismo efecto sobre la reposición.

## Interfaces

| Interlocutor | Canal | Propósito |
|---|---|---|
| `smartstock-inventario` | Kafka | Recibir `inventory.stock_low.v1`. |
| `smartstock-gateway` | Kafka | Intercambiar información de compras, solicitudes y cambios de estado de las órdenes. |

Los esquemas compartidos de los mensajes se versionan en `smartstock-infra/contracts/`.

## Desarrollo y despliegue

Este repositorio mantiene el servicio .NET, sus migraciones de PostgreSQL, sus pruebas, su Dockerfile y su pipeline. La cobertura mínima exigida es del 60 %. Las pruebas cubren la validación de cotizaciones, las transiciones de estado y la recepción duplicada de eventos. Docker Compose y Kubernetes se coordinan desde `smartstock-infra`.
