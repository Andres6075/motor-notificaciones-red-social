#  Motor de Notificaciones en Tiempo Real – Red Social

> **ITY1101 – Gestión de Datos para IA** | Evaluación Parcial N°3 | Caso de Estudio 2

---

## Equipo

| Nombre | Rol | Responsabilidades |
|--------|-----|-------------------|
| **Leandro Marín** | Project Manager / Jefe de Grupo | Planificación del proyecto, coordinación de entregas, informe técnico, plan de seguridad, modelo IA y monitoreo |
| **Andrés Zúñiga** | Data Engineer | Implementación del pipeline completo: ingesta, limpieza, validación, modelo IA, carga y configuración del entorno Docker |

---

## Descripción del Proyecto

Este proyecto consiste en el diseño e implementación de un **pipeline DataOps** para el procesamiento de eventos en tiempo real en una red social. Cuando un usuario realiza una acción (like, comentario o nuevo seguidor), el sistema captura ese evento, lo limpia, lo valida, lo clasifica con un modelo de IA y lo almacena para que la notificación llegue al usuario destinatario en menos de 500 ms.

El proyecto aplica una **metodología adaptativa (Scrum)** con sprints de 2 semanas, lo que permite iterar rápidamente sobre las funcionalidades del sistema según el feedback del equipo de producto.

---

## Arquitectura del Pipeline

```
[Eventos de usuarios]
        ↓
  [Apache Kafka]              ← Etapa 1: Ingesta en tiempo real
        ↓
  [Apache Spark]              ← Etapa 2: Limpieza y transformación
        ↓
[Great Expectations]          ← Etapa 3: Validación estructural y semántica
        ↓
  [Modelo IA – Random Forest] ← Etapa 4: Clasificación spam/bot (NUEVO Parcial 3)
        ↓                            ↓
[Redis / PostgreSQL]          [Cola eventos-spam] ← Bloqueados
        ↓
[Grafana / Prometheus]        ← Monitoreo y KPIs
        ↓
   [Metabase BI]              ← Dashboard interactivo (NUEVO Parcial 3)
```

---

## Estructura del Repositorio

```
motor-notificaciones-red-social/
│
├── src/
│   ├── ingesta/
│   │   └── kafka_producer.py            # Etapa 1: Productor de eventos hacia Kafka
│   ├── limpieza/
│   │   └── spark_cleaner.py             # Etapa 2: Limpieza y transformación con Spark
│   ├── validacion/
│   │   └── great_expectations_suite.py  # Etapa 3: Validación estructural y semántica
│   ├── modelo/
│   │   └── modelo_spam.py               # Etapa 4: Clasificación spam/bot con Random Forest (NUEVO)
│   └── carga/
│       └── db_loader.py                 # Etapa 5: Carga a Redis y PostgreSQL
│
├── docker/
│   ├── Dockerfile                       # Imagen del pipeline
│   └── docker-compose.yml               # Orquestación de servicios
│
├── logs/
│   └── pipeline_execution.log           # Log de ejecución del pipeline
│
├── data/
│   ├── raw/
│   │   └── eventos_red_social.json      # Datos reales capturados por el pipeline
│   ├── processed/                       # Datos limpios y transformados
│   └── validated/                       # Datos validados listos para clasificación
│
├── requirements.txt                     # Dependencias Python del proyecto
└── README.md                            # Este archivo
```

---

## Requisitos Previos

- **Docker Desktop** (versión 24.0 o superior)
- **Python 3.10+**
- **Git**

---

## Instalación y Ejecución

### 1. Clonar el repositorio

```bash
git clone https://github.com/Andres6075/motor-notificaciones-red-social.git
cd motor-notificaciones-red-social
```

### 2. Instalar dependencias Python

```bash
pip install -r requirements.txt
```

### 3. Levantar el entorno con Docker

```bash
docker-compose -f docker/docker-compose.yml up -d
```

Servicios levantados:
- **Kafka** en `localhost:29092`
- **Redis** en `localhost:6379`
- **PostgreSQL** en `localhost:5432`

### 4. Ejecutar el pipeline 

```bash
# Etapa 1: Ingesta de eventos
python src/ingesta/kafka_producer.py
```
```bash
# Etapa 2: Limpieza y transformación
python src/limpieza/spark_cleaner.py
```
```bash
# Etapa 3: Validación de datos
python src/validacion/great_expectations_suite.py
```
```bash
# Etapa 4: Clasificación spam/bot con Modelo IA (NUEVO)
python src/modelo/modelo_spam.py
```
```bash
# Etapa 5: Carga a Redis y PostgreSQL
python src/carga/db_loader.py
```

---

## Etapas del Pipeline

### 1. Ingesta (`src/ingesta/`)
Captura eventos en tiempo real usando **Apache Kafka**. Formato JSON:
```json
{
  "evento_id": "evt-001",
  "tipo_evento": "like",
  "usuario_origen": "user_001",
  "usuario_destino": "user_042",
  "publicacion_id": "post_789",
  "timestamp": "2025-05-07T10:32:01Z"
}
```

### 2. Limpieza (`src/limpieza/`)
Con **Apache Spark Streaming**:
- Eliminación de duplicados por ID único
- Descarte de campos obligatorios nulos
- Normalización de timestamps a ISO 8601
- Estandarización del campo `tipo_evento`

### 3. Validación (`src/validacion/`)
Con **Great Expectations**:
- Validaciones estructurales: tipos de dato, campos obligatorios
- Validaciones semánticas: fechas no futuras, IDs existentes, no auto-interacción

### 4. Modelo IA (`src/modelo/`) ← NUEVO Parcial 3
Clasificador **Random Forest** que detecta eventos spam/bot:
- Se entrena con datos históricos del pipeline (1.000 registros)
- Clasifica cada evento en tiempo real antes de la carga
- Eventos legítimos → tópico `eventos-clasificados` → carga
- Eventos spam/bot → tópico `eventos-spam` → bloqueados

**Métricas del modelo:**
| Métrica | Valor |
|---------|-------|
| Accuracy | 95.5% |
| Precision | 94.2% |
| Recall | 92.9% |
| F1-Score | 93.5% |
| ROC-AUC | 0.990 |
| Gini | 0.980 |

### 5. Carga (`src/carga/`)
Los eventos aprobados por el modelo se cargan en:
- **Redis**: notificaciones pendientes (baja latencia)
- **PostgreSQL**: historial completo de eventos procesados

---

## KPIs de Monitoreo

| KPI | Umbral Crítico | Herramienta |
|-----|----------------|-------------|
| Latencia de notificación | > 500 ms | Grafana |
| Completitud de eventos | < 99.9% | Prometheus |
| Tasa de errores de validación | > 0.5% | Great Expectations |
| Throughput del pipeline | < 1.000 ev/seg | Grafana |
| Errores de carga a BD | > 0.1% | Logs PostgreSQL |
| Tasa de spam detectado | > 10% | Modelo IA + Metabase |

---

## Seguridad

- **Cifrado en tránsito**: TLS 1.3 entre todos los servicios
- **Cifrado en reposo**: AES-256 para datos en Redis y PostgreSQL
- **Control de acceso**: RBAC con permisos mínimos por componente
- **Enmascaramiento**: Solo se usan IDs internos, sin datos personales en el pipeline
- **Cumplimiento legal**: Ley 19.628 (Chile) y GDPR

---

## Bibliografía

- Apache Kafka Documentation: https://kafka.apache.org/documentation/
- Apache Spark Documentation: https://spark.apache.org/docs/latest/
- Great Expectations Documentation: https://docs.greatexpectations.io/
- scikit-learn Documentation: https://scikit-learn.org/stable/
- Redis Documentation: https://redis.io/docs/
- PostgreSQL Documentation: https://www.postgresql.org/docs/
- Ley 19.628 – Protección de la Vida Privada (Chile)

---

*ITY1101 – Gestión de Datos para IA | Sección 301D | 2026*
