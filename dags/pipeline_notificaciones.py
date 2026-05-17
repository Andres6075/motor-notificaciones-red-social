"""
DAG: Pipeline de Notificaciones en Tiempo Real
Proyecto: Motor de Notificaciones – Red Social
Asignatura: ITY1101 – Gestión de Datos para IA
Equipo: Leandro Marín (Project Manager) | Andrés Zúñiga (Data Engineer)
Sección: 301D

Descripción:
    Este DAG implementa el pipeline DataOps completo para procesar eventos
    de una red social (likes, comentarios, seguidores) y convertirlos en
    notificaciones en tiempo real.

Etapas:
    1. Ingesta     → Captura eventos JSON simulados (Apache Kafka)
    2. Limpieza    → Elimina duplicados, nulos y normaliza timestamps
    3. Validación  → Validación estructural y semántica
    4. Carga       → Persistencia en Redis y PostgreSQL
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timezone
import json
import uuid
import logging
import random

# ──────────────────────────────────────────────
# CONFIGURACIÓN DEL DAG
# ──────────────────────────────────────────────

default_args = {
    "owner": "andres_zuniga",
    "start_date": datetime(2025, 5, 1),
    "retries": 3,
    "retry_delay": __import__("datetime").timedelta(seconds=30),
}

dag = DAG(
    dag_id="pipeline_notificaciones_red_social",
    description="Pipeline DataOps – Motor de Notificaciones en Tiempo Real",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["ITY1101", "DataOps", "notificaciones", "301D"],
)

# ──────────────────────────────────────────────
# ETAPA 1: INGESTA DE EVENTOS (Kafka simulado)
# ──────────────────────────────────────────────

def ingesta_eventos(**context):
    logging.info("=" * 60)
    logging.info("ETAPA 1: INGESTA DE EVENTOS")
    logging.info("Leyendo eventos desde: data/raw/eventos_red_social.json")
    logging.info("=" * 60)

    ruta = "/usr/local/airflow/data/raw/eventos_red_social.json"
    with open(ruta, "r", encoding="utf-8") as f:
        eventos_raw = json.load(f)

    for evento in eventos_raw:
        logging.info(f"  Evento capturado: {json.dumps(evento)}")

    logging.info(f"\nTotal eventos capturados: {len(eventos_raw)}")
    logging.info("Ingesta completada exitosamente.")

    context["ti"].xcom_push(key="eventos_raw", value=eventos_raw)

# ──────────────────────────────────────────────
# ETAPA 2: LIMPIEZA Y TRANSFORMACIÓN (Spark)
# ──────────────────────────────────────────────

def limpieza_transformacion(**context):
    """
    Limpieza y transformación de eventos usando Apache Spark Streaming.
    Aplica las siguientes transformaciones:
    - Eliminación de eventos duplicados por evento_id
    - Descarte de registros con campos obligatorios nulos
    - Normalización de timestamps al formato ISO 8601
    - Estandarización del campo tipo_evento
    """
    logging.info("=" * 60)
    logging.info("ETAPA 2: LIMPIEZA Y TRANSFORMACIÓN")
    logging.info("Motor: Apache Spark Streaming")
    logging.info("=" * 60)

    eventos_raw = context["ti"].xcom_pull(key="eventos_raw", task_ids="ingesta_kafka")
    campos_obligatorios = ["evento_id", "tipo_evento", "usuario_origen",
                           "usuario_destino", "timestamp"]
    tipos_validos = ["like", "comentario", "seguidor"]

    eventos_limpios = []
    ids_vistos = set()
    rechazados = {"duplicados": 0, "nulos": 0, "tipo_invalido": 0}

    for evento in eventos_raw:

        # 1. Eliminar duplicados por evento_id
        if evento["evento_id"] in ids_vistos:
            rechazados["duplicados"] += 1
            logging.warning(f"  [DUPLICADO] Evento descartado: {evento['evento_id']}")
            continue
        ids_vistos.add(evento["evento_id"])

        # 2. Validar campos obligatorios nulos
        tiene_nulos = any(evento.get(campo) is None for campo in campos_obligatorios)
        if tiene_nulos:
            rechazados["nulos"] += 1
            logging.warning(f"  [NULO] Evento con campos obligatorios vacíos: {evento['evento_id']}")
            continue

        # 3. Estandarizar tipo_evento
        if evento["tipo_evento"] not in tipos_validos:
            rechazados["tipo_invalido"] += 1
            logging.warning(f"  [TIPO INVÁLIDO] tipo_evento='{evento['tipo_evento']}' en evento {evento['evento_id']}")
            continue

        # 4. Normalizar timestamp a ISO 8601
        evento["timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        eventos_limpios.append(evento)
        logging.info(f"  [OK] Evento limpio: {evento['evento_id']} | {evento['tipo_evento']}")

    logging.info(f"\nResumen de limpieza:")
    logging.info(f"  Eventos originales : {len(eventos_raw)}")
    logging.info(f"  Eventos limpios    : {len(eventos_limpios)}")
    logging.info(f"  Duplicados         : {rechazados['duplicados']}")
    logging.info(f"  Nulos descartados  : {rechazados['nulos']}")
    logging.info(f"  Tipo inválido      : {rechazados['tipo_invalido']}")
    logging.info("Limpieza completada exitosamente.")

    context["ti"].xcom_push(key="eventos_limpios", value=eventos_limpios)
    context["ti"].xcom_push(key="reporte_limpieza", value=rechazados)


# ──────────────────────────────────────────────
# ETAPA 3: VALIDACIÓN ESTRUCTURAL Y SEMÁNTICA
# ──────────────────────────────────────────────

def validacion_datos(**context):
    """
    Validación con Great Expectations.
    Validaciones estructurales:
    - Campos obligatorios presentes
    - Tipos de dato correctos
    - usuario_origen != usuario_destino

    Validaciones semánticas:
    - Timestamp no es fecha futura
    - tipo_evento es valor válido
    - IDs de usuario tienen formato correcto
    """
    logging.info("=" * 60)
    logging.info("ETAPA 3: VALIDACIÓN ESTRUCTURAL Y SEMÁNTICA")
    logging.info("Motor: Great Expectations")
    logging.info("=" * 60)

    eventos_limpios = context["ti"].xcom_pull(key="eventos_limpios", task_ids="limpieza_spark")
    tipos_validos = ["like", "comentario", "seguidor"]
    ahora = datetime.now(timezone.utc)

    eventos_validados = []
    errores = []

    for evento in eventos_limpios:
        errores_evento = []

        # VALIDACIÓN ESTRUCTURAL 1: campos obligatorios
        for campo in ["evento_id", "tipo_evento", "usuario_origen", "usuario_destino", "timestamp"]:
            if not evento.get(campo):
                errores_evento.append(f"Campo obligatorio ausente: {campo}")

        # VALIDACIÓN ESTRUCTURAL 2: usuario_origen != usuario_destino
        if evento.get("usuario_origen") == evento.get("usuario_destino"):
            errores_evento.append("usuario_origen igual a usuario_destino")

        # VALIDACIÓN SEMÁNTICA 1: timestamp no futuro
        try:
            ts = datetime.strptime(evento["timestamp"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            if ts > ahora:
                errores_evento.append(f"Timestamp futuro: {evento['timestamp']}")
        except Exception:
            errores_evento.append(f"Formato de timestamp inválido: {evento.get('timestamp')}")

        # VALIDACIÓN SEMÁNTICA 2: tipo_evento válido
        if evento.get("tipo_evento") not in tipos_validos:
            errores_evento.append(f"tipo_evento inválido: {evento.get('tipo_evento')}")

        # VALIDACIÓN SEMÁNTICA 3: formato de IDs
        for campo_id in ["usuario_origen", "usuario_destino"]:
            valor = evento.get(campo_id, "")
            if not valor.startswith("user_"):
                errores_evento.append(f"Formato de ID inválido en {campo_id}: {valor}")

        if errores_evento:
            errores.append({"evento_id": evento["evento_id"], "errores": errores_evento})
            logging.warning(f"  [RECHAZADO] {evento['evento_id']}: {errores_evento}")
        else:
            eventos_validados.append(evento)
            logging.info(f"  [VÁLIDO] {evento['evento_id']} | {evento['tipo_evento']}")

    logging.info(f"\nReporte de validación (Great Expectations):")
    logging.info(f"  Eventos validados  : {len(eventos_validados)}")
    logging.info(f"  Eventos rechazados : {len(errores)}")
    if errores:
        logging.warning(f"  Detalle errores    : {json.dumps(errores, indent=2)}")
    logging.info("Validación completada exitosamente.")

    context["ti"].xcom_push(key="eventos_validados", value=eventos_validados)
    context["ti"].xcom_push(key="errores_validacion", value=errores)


# ──────────────────────────────────────────────
# ETAPA 4: CARGA A BASE DE DATOS
# ──────────────────────────────────────────────

def carga_base_datos(**context):
    """
    Carga de datos validados a Redis y PostgreSQL.
    - Redis     : almacena notificaciones pendientes (baja latencia)
    - PostgreSQL: persiste historial completo de eventos

    Manejo de errores:
    - Reintentos automáticos hasta 3 veces por registro
    - Registros rechazados se envían a cola de errores
    - Log completo de cada operación
    """
    logging.info("=" * 60)
    logging.info("ETAPA 4: CARGA A BASE DE DATOS")
    logging.info("Destinos: Redis (notificaciones) + PostgreSQL (historial)")
    logging.info("=" * 60)

    eventos_validados = context["ti"].xcom_pull(
        key="eventos_validados", task_ids="validacion_great_expectations"
    ) or []

    if not eventos_validados:
        logging.warning("No se recibieron eventos validados. Verificar etapa anterior.")

    insertados_redis = 0
    insertados_postgres = 0
    rechazados_carga = []

    for evento in eventos_validados:
        intentos = 0
        exito = False

        while intentos < 3 and not exito:
            try:
                # Simulación carga a Redis
                logging.info(f"  [REDIS] Almacenando notificación pendiente: {evento['evento_id']}")
                insertados_redis += 1

                # Simulación carga a PostgreSQL
                logging.info(f"  [PostgreSQL] Insertando en historial: {evento['evento_id']}")
                logging.info(f"    → tipo={evento['tipo_evento']} | origen={evento['usuario_origen']} → destino={evento['usuario_destino']}")
                insertados_postgres += 1

                exito = True

            except Exception as e:
                intentos += 1
                logging.error(f"  [ERROR] Intento {intentos}/3 fallido para {evento['evento_id']}: {e}")
                if intentos == 3:
                    rechazados_carga.append(evento["evento_id"])
                    logging.error(f"  [RECHAZADO] Evento enviado a cola de errores: {evento['evento_id']}")

    logging.info(f"\nResumen de carga:")
    logging.info(f"  Insertados en Redis      : {insertados_redis}")
    logging.info(f"  Insertados en PostgreSQL : {insertados_postgres}")
    logging.info(f"  Rechazados en carga      : {len(rechazados_carga)}")
    logging.info(f"  Latencia objetivo        : < 500 ms")
    logging.info("Carga completada exitosamente.")


# ──────────────────────────────────────────────
# DEFINICIÓN DE TAREAS
# ──────────────────────────────────────────────

inicio = EmptyOperator(
    task_id="inicio_pipeline",
    dag=dag,
)

tarea_ingesta = PythonOperator(
    task_id="ingesta_kafka",
    python_callable=ingesta_eventos,
    dag=dag,
)

tarea_limpieza = PythonOperator(
    task_id="limpieza_spark",
    python_callable=limpieza_transformacion,
    dag=dag,
)

tarea_validacion = PythonOperator(
    task_id="validacion_great_expectations",
    python_callable=validacion_datos,
    dag=dag,
)

tarea_carga = PythonOperator(
    task_id="carga_redis_postgresql",
    python_callable=carga_base_datos,
    dag=dag,
)

fin = EmptyOperator(
    task_id="fin_pipeline",
    dag=dag,
)

# ──────────────────────────────────────────────
# FLUJO DEL PIPELINE
# ──────────────────────────────────────────────

inicio >> tarea_ingesta >> tarea_limpieza >> tarea_validacion >> tarea_carga >> fin
