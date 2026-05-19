# ============================================================
# Dockerfile – Motor de Notificaciones en Tiempo Real
# ITY1101 – Gestión de Datos para IA
# Autores: Leandro Marín & Andrés Zúñiga
# ============================================================

# CAPA 1: Imagen base con Python 3.10
# Usamos Python 3.10 por compatibilidad con kafka-python y psycopg2
FROM python:3.10-slim

# CAPA 2: Definir directorio de trabajo dentro del contenedor
# Todos los archivos del pipeline se copiarán aquí
WORKDIR /app

# CAPA 3: Copiar archivo de dependencias
# Se copia primero para aprovechar el caché de Docker
COPY requirements.txt .

# CAPA 4: Instalar dependencias Python
# kafka-python, redis, psycopg2 y great-expectations
RUN pip install --no-cache-dir -r requirements.txt

# CAPA 5: Copiar el código fuente del pipeline
# Incluye las 4 etapas: ingesta, limpieza, validación y carga
COPY src/ ./src/

# CAPA 6: Comando por defecto al iniciar el contenedor
# Inicia el productor de eventos Kafka (etapa de ingesta)
CMD ["python", "src/ingesta/kafka_producer.py"]