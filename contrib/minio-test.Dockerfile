FROM scratch
COPY minio /minio
ENTRYPOINT ["/minio"]
CMD ["server", "/data"]
