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

Este repositorio mantiene el servicio .NET, sus migraciones de PostgreSQL, sus pruebas, su Dockerfile y su pipeline. La cobertura mínima exigida es del 60 %. Las pruebas cubren validación de cotizaciones, transiciones y recepción duplicada de eventos y solicitudes. Docker Compose y Kubernetes se coordinan desde `smartstock-infra`.

## Primer bloque: estado y ejecución

El consumidor usa el contrato fijado en el submódulo `vendor/smartstock-infra`, revisión `c5867a8d48383dcadae2111eb2bf20770645e565`. Inicializarlo con `git submodule update --init --recursive` y comprobar la revisión con `git -C vendor/smartstock-infra rev-parse HEAD`.

El servicio requiere `DATABASE_URL` en formato de cadena de conexión Npgsql, `KAFKA_BOOTSTRAP_SERVERS` y, opcionalmente, `KAFKA_GROUP_ID` (predeterminado `smartstock-compras`). Al iniciar aplica la migración y siembra tres cotizaciones de ejemplo para `SKU-001`. Son datos, no reglas especiales para ese SKU.

```sh
docker build -t smartstock-compras .
docker run --rm --network <red-con-postgres-y-kafka> \
  -e 'DATABASE_URL=Host=<postgres>;Database=compras;Username=<usuario>;Password=<clave>' \
  -e 'KAFKA_BOOTSTRAP_SERVERS=<kafka>:9092' \
  smartstock-compras
```

La tabla de necesidades conserva los datos del evento y su posición Kafka original. Se inserta con una clave primaria sobre `event_id` dentro de una transacción y se confirma el offset solamente después del commit de PostgreSQL. La reentrega, incluida una posterior a un fallo al confirmar el offset, no crea otra necesidad. Un JSON, cruce o clave inválidos detienen el consumidor sin confirmar el mensaje: se requiere intervención operativa y no se debe avanzar manualmente el offset sin resolver el incidente. Los errores técnicos durante persistencia o confirmación se registran y reintentan; un reinicio puede provocar una reentrega segura.

Un operador puede verificar una necesidad ejecutando el comando del propio servicio, en el mismo entorno de red y con sus credenciales, sin conceder acceso a la base a otro microservicio:

```sh
docker run --rm --network <red-con-postgres-y-kafka> \
  -e 'DATABASE_URL=Host=<postgres>;Database=compras;Username=<usuario>;Password=<clave>' \
  smartstock-compras --inspect evt-sale-001
```

El resultado es JSON con el evento y su procedencia, o un mensaje de ausencia. Esta herramienta operativa no es una interfaz de integración.

## Puente Kafka Gateway–Compras (desarrollo local)

El segundo consumidor recibe `purchases.requests.v1` y publica `purchases.results.v1`, con clave `request_id`. Implementa `get_need`, `list_needs`, `list_offers`, `get_order`, `create_draft` y `decide_order`. Valida solicitudes y resultados con los esquemas JSON fijados, incluidos campos adicionales, rangos y fechas; rechaza claves discordantes y campos JSON duplicados. Un mensaje inválido o un ID reutilizado con contenido diferente detiene el servicio sin confirmar ese offset, para intervención operativa.

`PURCHASES_KAFKA_GROUP_ID` configura el grupo independiente de solicitudes (predeterminado `smartstock-compras-purchases`). `KAFKA_GROUP_ID` sigue perteneciendo al consumidor de inventario. Ambos usan `KAFKA_BOOTSTRAP_SERVERS`.

La migración `002` agrega órdenes, cursores y un diario de solicitudes/resultados. La identidad usa SHA-256 con comprobación explícita del identificador original; una colisión nunca sustituye una orden o resultado. El diario compara el documento JSON semánticamente, conserva también `occurred_at` y persiste el resultado exacto, incluida su fecha. Bloqueos transaccionales serializan duplicados y operaciones sobre una misma orden. El cambio de negocio, el cursor cuando corresponda y el resultado se confirman en una única transacción PostgreSQL.

El diario actúa como outbox durable: primero se confirma PostgreSQL, después se publica el resultado esperando confirmación Kafka y solo entonces se confirma el offset de la solicitud. Tras un fallo entre pasos, la reentrega publica exactamente el resultado almacenado sin repetir efectos. La entrega es al menos una vez; Gateway debe persistir y deduplicar resultados. No se deben expirar ni limpiar el diario o los cursores durante v1. Si se pierde una respuesta, reenviar la solicitud original completa con el mismo ID; una lectura actualizada requiere otro ID.

Los listados ordenan por identificador ascendente con colación PostgreSQL `C`, respetan `limit` y devuelven `next_cursor: null` al final. Los cursores son tokens aleatorios persistidos y ligados a operación y SKU; no aceptan un token modificado o de otro contexto. No expiran ni ofrecen una instantánea entre páginas.

Un borrador exige necesidad y oferta existentes, SKU iguales, cantidad mínima y moneda reconocida por los datos ISO de .NET. Guarda la oferta completa como instantánea y empieza en `DRAFT`, revisión 1. Reutilizar `order_id` mediante otra solicitud devuelve `CONFLICT`. La decisión exige revisión actual y estado `DRAFT`; solo un cambio concurrente puede ganar y produce revisión 2, estado `APPROVED`/`REJECTED` e identidad del operador.

**Las decisiones están deshabilitadas de forma predeterminada.** Solo para demostraciones locales se permiten con las dos opciones explícitas y un actor exacto:

```text
PURCHASES_ENVIRONMENT=local
PURCHASES_ALLOW_LOCAL_DECISIONS=true
PURCHASES_LOCAL_ACTOR_ID=local-operator
```

Sin ambas opciones, o si `actor_id` no coincide con el actor configurado (predeterminado `local-operator`), se conserva un resultado `FORBIDDEN`. Habilitar después la opción no cambia ese resultado: una decisión nueva requiere otro `request_id`. Este interruptor no autentica personas ni servicios y **no es apto para producción**. La autenticación de Kafka, ACL y propagación verificable de identidad siguen pendientes del mecanismo acordado entre servicios. Un agente MCP no debe decidir órdenes.

Para pruebas con una base PostgreSQL de prueba aislada:

```sh
TEST_DATABASE_URL='Host=localhost;Database=compras_test;Username=postgres;Password=<clave>' \
  TEST_KAFKA_BOOTSTRAP_SERVERS='localhost:9092' \
  dotnet test tests/SmartStock.Compras.Tests --collect:"XPlat Code Coverage" --results-directory TestResults
```

Las pruebas usan una base y un broker aislados. Cubren validación de las seis operaciones, permisos locales, SKU/mínimos/monedas, instantánea de ofertas, paginación y contexto de cursores, duplicados concurrentes, resultados inmutables, decisiones concurrentes y rollback del negocio si falla el diario. Las pruebas Kafka verifican la reentrega del resultado durable, mensajes inválidos/conflictivos sin avance de offsets y recuperación tras un bloqueo temporal de PostgreSQL, además del flujo de inventario existente. Publicar un mensaje no constituye una recepción aplicada. No ejecutar estas pruebas sobre bases o topics compartidos de un despliegue: algunas fuerzan bloqueos y restricciones temporales.

### Compatibilidad pendiente de coordinación

El contrato v1 no limita la longitud o los caracteres de los identificadores y permite fechas RFC 3339. La persistencia de inventario existente utiliza `text` de PostgreSQL indexado para `event_id` y `DateTimeOffset`/`timestamptz` para la fecha: no soporta identificadores con NUL ni identificadores de tamaño superior al límite de índice de PostgreSQL; también restringe algunas representaciones RFC 3339, como segundos intercalares, y la precisión de la fecha persistida es de microsegundos. El puente evita el límite del índice para `request_id` y `order_id` mediante claves hash y conserva las fechas de solicitudes sin conversión, pero PostgreSQL `text`/`jsonb` sigue sin admitir NUL y la infraestructura Kafka limita el tamaño de mensajes. Hay que acordar con Infra e Inventario cómo cubrir el contrato completo antes de declarar interoperabilidad general; no debe asumirse que estas restricciones forman parte del contrato compartido.
