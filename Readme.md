# Motor de Notificaciones en Tiempo Real – Red Social

> **ITY1101 – Gestión de Datos para IA** | Evaluación Parcial N°2 | Caso de Estudio 2

---

# Equipo

| Nombre | Rol | Responsabilidades |
|--------|-----|-------------------|
| **Leandro Marín** | Project Manager / Jefe de Grupo | Planificación del proyecto, coordinación de entregas, informe técnico, plan de seguridad y monitoreo |
| **Andrés Zúñiga** | Data Engineer | Implementación del pipeline completo: ingesta, limpieza, validación, carga y configuración del entorno Docker |

---

# Descripción del Proyecto

Este proyecto consiste en el diseño e implementación de un **pipeline DataOps** para el procesamiento de eventos en tiempo real en una red social. Cuando un usuario realiza una acción (like, comentario o nuevo seguidor), el sistema captura ese evento, lo limpia, lo valida y lo almacena para que la notificación llegue al usuario destinatario en menos de 500 ms.

El proyecto aplica una **metodología adaptativa (Scrum)** con sprints de 2 semanas, lo que permite iterar rápidamente sobre las funcionalidades del sistema según el feedback del equipo de producto.

---

# Arquitectura del Pipeline

```
[Eventos de usuarios]
        ↓
  [Apache Kafka]         ← Ingesta en tiempo real
        ↓
  [Apache Spark]         ← Limpieza y transformación
        ↓
[Great Expectations]     ← Validación estructural y semántica
        ↓
  [Redis / PostgreSQL]   ← Carga y persistencia
        ↓
[Grafana / Prometheus]   ← Monitoreo y KPIs
```

---

# Estructura del Repositorio

```
motor-notificaciones-red-social/
│
├── src/
│   ├── ingesta/
│   │   └── kafka_producer.py       # Script productor de eventos hacia Kafka
│   ├── limpieza/
│   │   └── spark_cleaner.py        # Limpieza y transformación con Spark
│   ├── validacion/
│   │   └── great_expectations_suite.py  # Reglas de validación estructural y semántica
│   └── carga/
│       └── db_loader.py            # Carga a Redis y PostgreSQL con manejo de errores
│
├── docker/
│   ├── Dockerfile                  # Imagen del pipeline
│   └── docker-compose.yml          # Orquestación de servicios (Kafka, Spark, Redis, PostgreSQL)
│
├── logs/
│   └── pipeline_execution.log      # Log de ejemplo de ejecución del pipeline
│
├── data/
│   ├── raw/                        # Datos crudos de entrada
│   ├── processed/                  # Datos limpios y transformados
│   └── validated/                  # Datos validados listos para carga
│
├── docs/
│   └── diagrama_pipeline.png       # Diagrama visual del flujo de datos
│
├── requirements.txt                # Dependencias Python del proyecto
└── README.md                       # Este archivo
```

---

# Requisitos Previos

Antes de ejecutar el proyecto, asegúrate de tener instalado:

- **Docker Desktop** (versión 24.0 o superior)
- **Python 3.10+**
- **Git**

---

# Instalación y Ejecución

# 1. Clonar el repositorio

```bash
git clone https://github.com/[usuario-equipo]/motor-notificaciones-red-social.git
cd motor-notificaciones-red-social
```

# 2. Instalar dependencias Python

```bash
pip install -r requirements.txt
```

# 3. Levantar el entorno con Docker

```bash
docker-compose -f docker/docker-compose.yml up -d

Esto levanta automáticamente los siguientes servicios:
- **Kafka** en `localhost:9092`
- **Redis** en `localhost:6379`
- **PostgreSQL** en `localhost:5432`

# 4. Ejecutar el pipeline completo

```bash
# Paso 1: Iniciar la ingesta de eventos
python src/ingesta/kafka_producer.py

# Paso 2: Ejecutar limpieza y transformación
python src/limpieza/spark_cleaner.py

# Paso 3: Validar los datos
python src/validacion/great_expectations_suite.py

# Paso 4: Cargar a base de datos
python src/carga/db_loader.py
```

---

# Etapas del Pipeline

# 1. Ingesta (`src/ingesta/`)
Captura eventos en tiempo real desde la plataforma usando **Apache Kafka**. Cada evento tiene el siguiente formato JSON:

```json
{
  "tipo_evento": "like",
  "usuario_origen": "user_001",
  "usuario_destino": "user_042",
  "publicacion_id": "post_789",
  "timestamp": "2025-05-07T10:32:01Z"
}
```

### 2. Limpieza y Transformación (`src/limpieza/`)
Usando **Apache Spark Streaming** se aplican las siguientes transformaciones:
- Eliminación de eventos duplicados por ID único
- Descarte de registros con campos obligatorios nulos
- Normalización de timestamps al formato ISO 8601
- Estandarización del campo `tipo_evento`

### 3. Validación (`src/validacion/`)
Con **Great Expectations** se aplican validaciones:
- **Estructurales**: tipos de dato, campos obligatorios, unicidad de claves
- **Semánticas**: fechas no futuras, tipos de evento válidos, IDs de usuario existentes

Los eventos que no pasan la validación se envían a una cola de errores sin detener el pipeline.

### 4. Carga (`src/carga/`)
Los datos validados se cargan en dos destinos:
- **Redis**: almacena notificaciones pendientes de envío (baja latencia)
- **PostgreSQL**: persiste el historial completo de eventos procesados

El script implementa reintentos automáticos (hasta 3 intentos) y registro de registros rechazados.

---

## 📊 KPIs de Monitoreo

| KPI | Umbral Crítico | Herramienta |
|-----|----------------|-------------|
| Latencia de notificación | > 500 ms | Grafana |
| Completitud de eventos | < 99.9% | Prometheus |
| Tasa de errores de validación | > 0.5% | Great Expectations |
| Throughput del pipeline | < 1.000 ev/seg | Grafana |
| Errores de carga a BD | > 0.1% | Logs PostgreSQL |

---

## 🔒 Seguridad

- **Cifrado en tránsito**: TLS 1.3 entre todos los servicios
- **Cifrado en reposo**: AES-256 para datos en Redis y PostgreSQL
- **Control de acceso**: RBAC con permisos mínimos por componente
- **Enmascaramiento**: Solo se usan IDs internos, sin datos personales en el pipeline
- **Cumplimiento legal**: Ley 19.628 (Chile) y GDPR

---

## 📅 Planificación

| Entregable | Fecha |
|------------|-------|
| Avance 1 | Jueves 7 de mayo 2025 |
| Avance 2 | Jueves 14 de mayo 2025 |
| Entrega Final + Presentación | Martes 19 de mayo 2025 |

---

## 📚 Bibliografía

- Apache Kafka Documentation: https://kafka.apache.org/documentation/
- Apache Spark Documentation: https://spark.apache.org/docs/latest/
- Great Expectations Documentation: https://docs.greatexpectations.io/
- Redis Documentation: https://redis.io/docs/
- PostgreSQL Documentation: https://www.postgresql.org/docs/
- Ley 19.628 – Protección de la Vida Privada (Chile)

---

*ITY1101 – Gestión de Datos para IA | Sección 301D | 2025*
