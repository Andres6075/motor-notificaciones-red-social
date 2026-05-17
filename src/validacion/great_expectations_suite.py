"""
great_expectations_suite.py
Validación estructural y semántica de eventos con Great Expectations.
Etapa 3 del pipeline DataOps – Motor de Notificaciones.
Responsable: Leandro Marín
"""

import json
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BROKER     = "localhost:29092"
TOPIC_ENTRADA    = "eventos-limpios"
TOPIC_VALIDADO   = "eventos-validados"
TOPIC_RECHAZADOS = "eventos-rechazados"

TIPOS_VALIDOS        = {"like", "comentario", "seguidor"}
USUARIOS_REGISTRADOS = {f"user_{i:03d}" for i in range(1, 101)}

def validar_estructura(evento: dict):
    errores = []
    campos_string = ["evento_id", "tipo_evento", "usuario_origen",
                     "usuario_destino", "timestamp"]
    for campo in campos_string:
        if campo not in evento:
            errores.append(f"[ESTRUCTURAL] Campo faltante: {campo}")
        elif not isinstance(evento[campo], str):
            errores.append(f"[ESTRUCTURAL] Tipo incorrecto en '{campo}'")
    return errores

def validar_semantica(evento: dict):
    errores = []
    if evento.get("tipo_evento") not in TIPOS_VALIDOS:
        errores.append(f"[SEMÁNTICA] tipo_evento inválido: {evento.get('tipo_evento')}")
    try:
        ts = datetime.strptime(evento["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
        ts = ts.replace(tzinfo=timezone.utc)
        if ts > datetime.now(timezone.utc):
            errores.append(f"[SEMÁNTICA] Timestamp en el futuro: {evento['timestamp']}")
    except (ValueError, KeyError):
        errores.append(f"[SEMÁNTICA] Timestamp inválido: {evento.get('timestamp')}")
    origen  = evento.get("usuario_origen", "")
    destino = evento.get("usuario_destino", "")
    if origen not in USUARIOS_REGISTRADOS:
        errores.append(f"[SEMÁNTICA] usuario_origen no registrado: {origen}")
    if destino not in USUARIOS_REGISTRADOS:
        errores.append(f"[SEMÁNTICA] usuario_destino no registrado: {destino}")
    if origen == destino:
        errores.append("[SEMÁNTICA] usuario_origen igual a usuario_destino")
    return errores

def main():
    print(f"[VALIDACIÓN] Conectando a Kafka en {KAFKA_BROKER}...")
    consumer = KafkaConsumer(
        TOPIC_ENTRADA,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="validacion-group",
        api_version=(2, 5, 0)
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        api_version=(2, 5, 0)
    )
    print("[VALIDACIÓN] Escuchando eventos limpios...\n")
    validados  = 0
    rechazados = 0
    try:
        for mensaje in consumer:
            evento = mensaje.value
            errores = validar_estructura(evento) + validar_semantica(evento)
            if not errores:
                producer.send(TOPIC_VALIDADO, value=evento)
                validados += 1
                print(f"  ✔ Validado [{validados}]: {evento['tipo_evento']} | "
                      f"{evento['usuario_origen']} → {evento['usuario_destino']}")
            else:
                producer.send(TOPIC_RECHAZADOS, value={"evento_original": evento, "errores": errores})
                rechazados += 1
                print(f"  ✘ Rechazado [{rechazados}]: {errores}")
    except KeyboardInterrupt:
        print(f"\n[VALIDACIÓN] Detenido. Validados: {validados} | Rechazados: {rechazados}")
    finally:
        consumer.close()
        producer.close()

if __name__ == "__main__":
    main()