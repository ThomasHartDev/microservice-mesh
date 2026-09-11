# syntax=docker/dockerfile:1
FROM python:3.12-alpine AS build
WORKDIR /src
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
COPY services/notifications/worker.py /src/worker.py
COPY python/observability.py /src/observability.py
RUN python -m compileall /src/worker.py /src/observability.py

FROM python:3.12-alpine
RUN addgroup -g 65532 -S app && adduser -S -D -H -u 65532 -G app app
WORKDIR /app
COPY --from=build --chown=65532:65532 /src/worker.py /app/worker.py
COPY --from=build --chown=65532:65532 /src/observability.py /app/observability.py
USER 65532:65532
ENV PYTHONUNBUFFERED=1
ENV MESH_SERVICE=notifications
CMD ["python", "-u", "/app/worker.py"]
