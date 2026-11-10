# Contrato de coordinación

La aplicación implementa `storage.Coordinator` para compartir cuotas entre
procesos. La implementación en memoria permite compartir estado entre varias
instancias del mismo proceso; si no se proporciona coordinador se crea uno
privado para cada `Storage`.

## Estado y operaciones atómicas

El estado persistente mínimo por scope contiene límites, tamaños confirmados
por clave, reservas pendientes y eventos admitidos para la ventana de operaciones.
Los tamaños y sumas deben validarse para impedir desbordamientos de enteros.

`Initialize` se ejecuta antes de usar la instancia. Debe inicializar un scope una
sola vez y llamar a `scan` bajo exclusión de inicialización. Las instancias
posteriores deben comprobar que los límites coinciden y reutilizar el estado;
no deben sobrescribirlo con otro listado ni borrar reservas activas. Si la
inicialización falla, debe poder reintentarse. El namespace puede comenzar por
encima de su cuota: no se permite crecer, pero sí reducir o borrar objetos.

`Allow` admite una operación y actualiza atómicamente la ventana compartida.
Una llamada admitida no se devuelve al presupuesto aunque falle después.
La ventana es deslizante y abarca todas las operaciones lógicas públicas;
las peticiones internas de S3 y los escaneos iniciales quedan fuera.

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
no pueden demostrarlo deben envolver `ErrOutcomeUnknown`. El backend S3 trata
conservadoramente cualquier error posterior a invocar la mutación del SDK como
incierto, porque los reintentos pueden ocultar un intento exitoso anterior.

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
