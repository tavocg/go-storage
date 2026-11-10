# Contrato de coordinación

La aplicación implementa `storage.Coordinator` para compartir cuotas entre
procesos. La implementación en memoria permite compartir estado entre varias
instancias del mismo proceso; si no se proporciona coordinador se crea uno
privado para cada `Storage`.

## Estado y operaciones atómicas

El estado persistente mínimo por scope de bytes contiene límites, tamaños
confirmados por clave y reservas pendientes.
Los tamaños y sumas deben validarse para impedir desbordamientos de enteros.

`Initialize` se ejecuta antes de usar la instancia. Debe inicializar un scope una
sola vez y llamar a `scan` bajo exclusión de inicialización. Las instancias
posteriores deben comprobar que los límites coinciden y reutilizar el estado;
no deben sobrescribirlo con otro listado ni borrar reservas activas. Si la
inicialización falla, debe poder reintentarse. El namespace puede comenzar por
encima de su cuota: no se permite crecer, pero sí reducir o borrar objetos.

`Reserve` debe, en una sola transacción:

1. Comprobar que no exista otra mutación pendiente sobre la misma clave.
2. Leer el tamaño confirmado anterior de esa clave.
3. Para Put, calcular `delta = max(tamaño_nuevo - tamaño_anterior, 0)`.
   Para Delete, delta es cero.
4. Comprobar que `ocupado + reservado + delta <= máximo` si delta es positivo.
5. Registrar el ID, operación, clave, tamaño anterior y delta, y adquirir su
   exclusión de mutación hasta resolver el resultado.

El cuerpo ya está preparado y su tamaño es exacto. No se mantiene una transacción
de base de datos abierta durante la transferencia; la reserva es un registro
durable. Las demás claves siguen pudiendo reservar y subir.

Los IDs de mutación son aleatorios y únicos. Si `Reserve` tiene un resultado
incierto en la base de datos, el coordinador debe reconciliar cualquier reserva
huérfana: la librería no empieza una escritura cuando Reserve devuelve error.
Nunca se debe devolver una reserva utilizable antes de persistirla.

`Reservation.Complete(Applied)` aplica el cambio confirmado, descuenta la reserva
y libera la clave en una transacción. `Complete(Unchanged)` solo devuelve la
reserva y libera la clave, conservando el objeto anterior. Ambas operaciones
son idempotentes para el mismo resultado y rechazan un resultado contradictorio.
Un segundo Delete de una clave ausente no descuenta bytes de otro objeto.
La aplicación debe conservar suficiente historial para reintentar confirmaciones
tras una respuesta perdida sin aplicarlas dos veces.

El mutex del coordinador en memoria protege las transiciones cortas de estado;
la inicialización mantiene una exclusión por scope durante el escaneo sin
bloquear otros scopes. Las transferencias
no mantienen ese mutex. Las mutaciones de claves ocupadas se rechazan con
`ErrObjectBusy`; no se encolan.

## Errores y recuperación

Los backends garantizan que un error ordinario no modificó el objeto. Cuando
no pueden demostrarlo deben envolver `ErrOutcomeUnknown`. El backend S3 conserva el resultado como incierto si hubo algún intento HTTP
enviado y la llamada terminó con error. Si se rechazó antes del primer envío,
no hubo cambio. Un reintento bloqueado puede ocultar un intento exitoso anterior.

- Error conocido sin cambio: la librería completa con `Unchanged`.
- Éxito del backend: completa con `Applied`.
- Resultado incierto: devuelve `*PendingError` y conserva reserva y exclusión.
- Error de confirmación: devuelve `*PendingError` con `Suggested` igual al
  resultado que debe reintentarse; este puede ya haberse aplicado remotamente.

Las confirmaciones normales usan un contexto independiente de la cancelación
del solicitante y un timeout de cinco segundos, configurable mediante
`FinalizeTimeout`. Si falla la confirmación, la librería no intenta subir otra vez.

```go
var pending *storage.PendingError
if errors.As(err, &pending) {
    if pending.Suggested != 0 {
        // Reintentar únicamente la finalización con un contexto vigente.
        err = pending.Reservation.Complete(ctx, pending.Suggested)
    } else {
        // Determinar primero si la mutación se aplicó y excluir cualquier
        // publicación tardía. Después confirmar Applied o Unchanged.
    }
}
```

No basta con que transcurra un TTL, se cancele el cliente o un listado no muestre
todavía el objeto. Una petición que sigue ejecutándose podría publicarlo después.
Antes de liberar una reserva incierta hay que impedir toda publicación tardía
del escritor original y determinar su resultado. Un coordinador persistente debe
ofrecer su propio mecanismo de inspección y recuperación por el ID expuesto en
`PendingError`, incluyendo operaciones cuyo proceso murió antes de devolver error.

S3 y la base de datos no comparten una transacción. La interfaz permite conservar
capacidad y bloquear una clave mientras se resuelve esa separación; no elimina
la necesidad de recuperación ni implementa automáticamente fencing en S3.
Las implementaciones de usuario deben cumplir ese contrato o conservar las
reservas inciertas indefinidamente. El coordinador en memoria hace esto último
hasta que se resuelva explícitamente a través de la reserva del error.

## Backends propios

Implementar `Backend` no requiere tipos AWS. Put recibe un lector seekable de
exactamente `Object.Size` bytes y debe reemplazar el objeto atómicamente. No debe
retener el lector cuando retorna. Get devuelve metadata y un cuerpo cerrable;
Delete debe ser idempotente y List devolver todos los objetos lógicos actuales.
La publicación y el borrado deben respetar la semántica de errores anterior.

Los límites contemplan cuerpos lógicos actuales. No equivalen a una cuota física
ni monetaria: archivos temporales, metadata, versiones históricas, partes de
multipart y operaciones internas del proveedor pueden consumir recursos extra.

## Presupuestos de operaciones en v3

`Storage` no admite ni cuenta operaciones. `storage.Limits` solo contiene
`MaxBytes` y `storage.Coordinator` ya no tiene Allow. Los backends reciben
`WithRateLimits(ratelimit.Options)` y usan un coordinador independiente:

```go
type Coordinator interface {
    Allow(ctx context.Context, scope string, category ratelimit.Category, limit ratelimit.Limit) error
}
```

Cada admisión debe comprobar y cargar atómicamente una ventana deslizante por
scope y categoría. Rechazar con `ratelimit.ErrLimited` sin esperar capacidad,
no devolver cargos cuando falle el envío y detectar límites incompatibles en
un presupuesto compartido. El coordinador no requiere escaneo del almacenamiento:
puede admitir peticiones incluso durante la inicialización de la cuota de bytes.
Una falla debe impedir el envío. Las categorías no configuradas son ilimitadas.
Todos los usuarios de un presupuesto deben configurar las mismas categorías y
límites; omitir una categoría desactiva su control en esa instancia.

En S3 se consulta antes de cada dispatch HTTP, incluyendo reintentos del SDK,
páginas e inicialización. Los clientes estándar HTTP también admiten cada
redirección. Los reenvíos ocultos dentro de un transporte personalizado o del
transporte subyacente quedan fuera de la visibilidad de esta capa; no deben
usarse para implementar reintentos de aplicación. El SDK sí queda instrumentado.
Filesystem cuenta una vez cada operación pública de backend, con un único
presupuesto `fs.Operations`.

Para R2, el scope de operaciones puede identificar la cuenta completa y abarcar
varios buckets. El scope de bytes sigue identificando un namespace concreto;
no debe reutilizarse para buckets diferentes. Un mismo coordinador de operaciones
puede tener presupuestos independientes para escrituras/listados y lecturas.
Los borrados son ilimitados salvo que se configure `s3.FreeRequests`.

Las implementaciones en memoria pierden el historial al reiniciar. Para mantener
límites entre procesos o reinicios hay que persistir eventos y reservas en los
respectivos coordinadores. Un límite por ocho horas no reemplaza la contabilidad
mensual del proveedor ni contempla clientes externos a estos backends.

## Migración desde v2

Cambiar imports a `/v3`, eliminar `storage.Limits.Rate` y trasladar sus
presupuestos al constructor del backend. Separar la implementación anterior
`storage.Coordinator.Allow` hacia `ratelimit.Coordinator.Allow`, que ahora recibe
categoría y límite. `s3.New` recibe `*awss3.Client`, no la antigua interfaz Client,
para garantizar que el SDK aplique las opciones de instrumentación de cada llamada.
