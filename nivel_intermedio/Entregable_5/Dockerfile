# Dockerfile — Microservicio de Inferencia
# Entregable 5 · Talento Tech · Sesión 21
#
# Construir:  docker build -t ml-inference .
# Ejecutar:   docker run -p 5000:5000 ml-inference
# Con Redis:  docker-compose up

FROM python:3.10-slim

# Variables de entorno
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=5000 \
    MODEL_PATH=/app/model.pkl \
    REDIS_HOST=redis \
    REDIS_PORT=6379

WORKDIR /app

# Instalar dependencias del sistema (mínimas)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copiar e instalar dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código
COPY app.py .

# Puerto expuesto
EXPOSE 5000

# Health check integrado en Docker
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:5000/health')"

# Ejecutar con gunicorn para producción
CMD ["gunicorn", "--workers", "2", "--bind", "0.0.0.0:5000", "--timeout", "60", "app:app"]
