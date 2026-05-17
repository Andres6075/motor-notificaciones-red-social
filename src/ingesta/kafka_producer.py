"""
kafka_producer.py
Ingesta de eventos en tiempo real hacia Apache Kafka.
Etapa 1 del pipeline DataOps – Motor de Notificaciones.
Responsable: Andrés Zúñiga
"""

import json
import time
import uuid
import random
from datetime import datetime, timezone
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:29092"
TOPIC_NAME   = "eventos-notificaciones"
EVENTOS_POR_LOTE = 3
INTERVALO_SEGUNDOS = 5
TOTAL_LOTES = 5

TIPOS_EVENTO = ["like", "comentario", "seguidor"]

def generar_evento():
    usuario_origen  = f"user_{random.randint(1, 100):03d}"
    usuario_destino = f"user_{random.randint(1, 100):03d}"
    while usuario_destino == usuario_origen:
        usuario_destino = f"user_{random.randint(1, 100):03d}"
    return {
        "evento_id":       str(uuid.uuid4()),
        "tipo_evento":     random.choice(TIPOS_EVENTO),
        "usuario_origen":  usuario_origen,
        "usuario_destino": usuario_destino,
        "publicacion_id":  f"post_{random.randint(1, 500):03d}",
        "timestamp":       datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    }

def main():
    print(f"[INGESTA] Conectando a Kafka en {KAFKA_BROKER}...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        api_version=(2, 5, 0)
    )
    print(f"[INGESTA] Productor conectado. Enviando eventos al tópico '{TOPIC_NAME}'...\n")
    try:
        for lote in range(1, TOTAL_LOTES + 1):
            print(f"[INGESTA] ── Lote #{lote} ──────────────────────────────")
            for _ in range(EVENTOS_POR_LOTE):
                evento = generar_evento()
                producer.send(TOPIC_NAME, value=evento)
                print(f"  → Evento enviado: {evento['tipo_evento']} | "
                      f"{evento['usuario_origen']} → {evento['usuario_destino']} | "
                      f"{evento['timestamp']}")
            producer.flush()
            print(f"[INGESTA] Lote #{lote} enviado correctamente.\n")
            if lote < TOTAL_LOTES:
                time.sleep(INTERVALO_SEGUNDOS)
    except KeyboardInterrupt:
        print("\n[INGESTA] Producción detenida por el usuario.")
    finally:
        producer.close()
        print(f"\n[INGESTA] Pipeline completado. Total: {TOTAL_LOTES} lotes, {TOTAL_LOTES * EVENTOS_POR_LOTE} eventos enviados.")

if __name__ == "__main__":
    main()