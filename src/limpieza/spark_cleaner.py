"""
spark_cleaner.py
Limpieza y transformación de eventos con Apache Spark Streaming.
Etapa 2 del pipeline DataOps – Motor de Notificaciones.
Responsable: Equipo (Leandro Marín + Andrés Zúñiga)
"""

import json
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BROKER        = "localhost:29092"
TOPIC_ENTRADA       = "eventos-notificaciones"
TOPIC_LIMPIO        = "eventos-limpios"
TOPIC_ERRORES       = "eventos-errores"
TIPOS_VALIDOS       = {"like", "comentario", "seguidor"}
CAMPOS_OBLIGATORIOS = {"evento_id", "tipo_evento", "usuario_origen",
                       "usuario_destino", "timestamp"}

ids_procesados = set()

def normalizar_timestamp(ts: str):
    formatos = [
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ]
    for fmt in formatos:
        try:
            dt = datetime.strptime(ts, fmt)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue
    return None

def limpiar_evento(evento: dict):
    faltantes = CAMPOS_OBLIGATORIOS - evento.keys()
    if faltantes:
        return None, f"Campos obligatorios faltantes: {faltantes}"
    for campo in CAMPOS_OBLIGATORIOS:
        if not evento.get(campo):
            return None, f"Campo nulo: {campo}"
    eid = evento["evento_id"]
    if eid in ids_procesados:
        return None, f"Evento duplicado: {eid}"
    ids_procesados.add(eid)
    ts_normalizado = normalizar_timestamp(evento["timestamp"])
    if not ts_normalizado:
        return None, f"Timestamp inválido: {evento['timestamp']}"
    evento["timestamp"] = ts_normalizado
    evento["tipo_evento"] = evento["tipo_evento"].strip().lower()
    if evento["tipo_evento"] not in TIPOS_VALIDOS:
        return None, f"Tipo de evento inválido: {evento['tipo_evento']}"
    if evento["usuario_origen"] == evento["usuario_destino"]:
        return None, "usuario_origen igual a usuario_destino"
    return evento, None

def main():
    print(f"[LIMPIEZA] Conectando a Kafka en {KAFKA_BROKER}...")
    consumer = KafkaConsumer(
        TOPIC_ENTRADA,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="limpieza-group",
        api_version=(2, 5, 0)
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        api_version=(2, 5, 0)
    )
    print("[LIMPIEZA] Escuchando eventos...\n")
    procesados = 0
    errores    = 0
    try:
        for mensaje in consumer:
            evento = mensaje.value
            evento_limpio, motivo_error = limpiar_evento(evento)
            if evento_limpio:
                producer.send(TOPIC_LIMPIO, value=evento_limpio)
                procesados += 1
                print(f"  ✔ Limpio [{procesados}]: {evento_limpio['tipo_evento']} | "
                      f"{evento_limpio['usuario_origen']} → {evento_limpio['usuario_destino']}")
            else:
                error_payload = {"evento_original": evento, "motivo": motivo_error}
                producer.send(TOPIC_ERRORES, value=error_payload)
                errores += 1
                print(f"  ✘ Error [{errores}]: {motivo_error}")
    except KeyboardInterrupt:
        print(f"\n[LIMPIEZA] Detenido. Procesados: {procesados} | Errores: {errores}")
    finally:
        consumer.close()
        producer.close()

if __name__ == "__main__":
    main()