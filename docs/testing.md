# Pruebas

La reescritura incluye una suite compartida para filesystem y S3, pruebas de
contabilidad en memoria y fallos deterministas de backend y coordinador.

```sh
go test -race ./...
go vet ./...
```

## MinIO en Docker

Las pruebas de integración crean buckets aislados y los eliminan al terminar.
Usan el cliente AWS normal, sin middleware especial de payload ni cambios a la
configuración global del entorno. Las subidas sin tamaño declarado se preparan
antes de enviar y tienen Content-Length; el lector es seekable para el SDK.

Para reproducir el entorno usado, compilar una versión fija de MinIO y construir
la imagen con [contrib/minio-test.Dockerfile](../contrib/minio-test.Dockerfile):

```sh
CGO_ENABLED=0 GOBIN=/tmp/go-storage-minio-bin go install github.com/minio/minio@v0.0.0-20260212201848-7aac2a2c5b7c
docker build -t go-storage-minio-test -f contrib/minio-test.Dockerfile /tmp/go-storage-minio-bin
docker run -d --rm --name go-storage-minio-test \
  -p 127.0.0.1:19000:9000 \
  -e MINIO_ROOT_USER=storage-test \
  -e MINIO_ROOT_PASSWORD=storage-test-secret \
  go-storage-minio-test
```

Esperar a que MinIO arranque y ejecutar:

```sh
MINIO_TEST_ENDPOINT=http://127.0.0.1:19000 go test -tags=integration -race -count=5 ./...
docker stop go-storage-minio-test
```

Sin `MINIO_TEST_ENDPOINT`, las pruebas MinIO se omiten. Sin la etiqueta
`integration`, el resto de pruebas no necesita red ni Docker. En entornos con
caché Go de solo lectura se puede añadir `GOCACHE=/tmp/go-storage-build`.

## Escenarios

- 64 subidas concurrentes sin tamaño declarado de 64 KiB, dos instancias con
  coordinador compartido y cuota de 512 KiB: exactamente ocho admitidas.
- Ventana de operaciones compartida entre instancias, con admisión concurrente.
- Lectura, listado, contenido, metadata, SHA-256, objetos vacíos y claves anidadas.
- Borrados repetidos, sobrescrituras que reducen tamaño y carga de estado existente.
- Sobrescritura detenida después de reservar: otra sobrescritura o borrado de la
  misma clave se rechaza, y su espacio anterior no se libera antes de tiempo.
- Subidas demasiado grandes sin truncar ni modificar el objeto existente.
- Resultado incierto tras publicar realmente en el backend: la cuota y la clave
  permanecen reservadas hasta resolverlo; la confirmación es idempotente.
- Error conocido, error de lectura y cancelación devuelven capacidad y limpian
  archivos temporales; cancelación no cancela la confirmación de limpieza.
- Fallo de coordinador bloquea la escritura. Fallo de confirmación permite
  reintentar solo esa fase. Resultado incierto sin publicación puede abortarse
  cuando se sabe que el escritor ya no puede publicar.
- Inicialización concurrente única, reservas preservadas, límites incompatibles,
  desbordamiento de enteros y recuperación de un namespace inicialmente excedido.
- Filesystem: confinamiento de claves, rechazo de symlinks y exclusión de staging.
- S3: inicialización y listado de 1005 objetos, verificando que la cuota incluya
  los objetos de todas las páginas.

Validación de esta reescritura: `go test -race ./...`, `go vet ./...` y cinco
repeticiones de `go test -tags=integration -race -count=5 ./...` contra MinIO
pasaron sin avisos del detector de carreras.

Las pruebas compartidas están en [internal/storagetest/suite.go](../internal/storagetest/suite.go).
La integración S3 está en [backends/s3/minio_integration_test.go](../backends/s3/minio_integration_test.go).

Las pruebas originales anteriores a la reescritura demostraron excedentes de
12 y 14 bytes con cuota de 10, y errores de Content-Length/hash en las subidas.
La suite actual exige corregir esos comportamientos en vez de aceptarlos.
No valida un coordinador distribuido de terceros, acceso real a AWS/R2, HTTPS,
caídas físicas de disco ni publicación tardía de un servidor remoto real.
