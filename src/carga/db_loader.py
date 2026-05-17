"""
db_loader.py
Carga de eventos validados a Redis y PostgreSQL.
Etapa 4 del pipeline DataOps – Motor de Notificaciones.
Responsable: Andrés Zúñiga
"""

import json
import time
import redis
import psycopg2
from kafka import KafkaConsumer

KAFKA_BROKER  = "localhost:9092"
TOPIC_ENTRADA = "eventos-validados"

REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_TTL  = 300

PG_HOST = "localhost"
PG_PORT = 5432
PG_DB   = "notificaciones_db"
PG_USER = "pipeline_user"
PG_PASS = "pipeline_pass"

MAX_REINTENTOS = 3

def conectar_redis():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping()
    print("[CARGA] Redis conectado.")
    return r

def conectar_postgres():
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    conn.autocommit = False
    print("[CARGA] PostgreSQL conectado.")
    return conn

def crear_tabla(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS eventos_procesados (
                evento_id        VARCHAR(36) PRIMARY KEY,
                tipo_evento      VARCHAR(20) NOT NULL,
                usuario_origen   VARCHAR(20) NOT NULL,
                usuario_destino  VARCHAR(20) NOT NULL,
                publicacion_id   VARCHAR(20),
                timestamp_evento TIMESTAMP WITH TIME ZONE NOT NULL,
                insertado_en     TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """)
    conn.commit()
    print("[CARGA] Tabla 'eventos_procesados' lista.\n")

def cargar_redis(r, evento: dict):
    clave = f"notif:{evento['usuario_destino']}:{evento['evento_id']}"
    for intento in range(1, MAX_REINTENTOS + 1):
        try:
            r.setex(clave, REDIS_TTL, json.dumps(evento))
            return True
        except Exception as e:
            print(f"  [REDIS] Intento {intento}/{MAX_REINTENTOS} fallido: {e}")
            time.sleep(0.5 * intento)
    return False

def cargar_postgres(conn, evento: dict):
    sql = """
        INSERT INTO eventos_procesados
            (evento_id, tipo_evento, usuario_origen, usuario_destino,
             publicacion_id, timestamp_evento)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (evento_id) DO NOTHING;
    """
    valores = (
        evento["evento_id"], evento["tipo_evento"],
        evento["usuario_origen"], evento["usuario_destino"],
        evento.get("publicacion_id"), evento["timestamp"]
    )
    for intento in range(1, MAX_REINTENTOS + 1):
        try:
            with conn.cursor() as cur:
                cur.execute(sql, valores)
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            print(f"  [POSTGRES] Intento {intento}/{MAX_REINTENTOS} fallido: {e}")
            time.sleep(0.5 * intento)
    return False

def main():
    print(f"[CARGA] Iniciando cargador. Broker: {KAFKA_BROKER}\n")
    r    = conectar_redis()
    conn = conectar_postgres()
    crear_tabla(conn)
    consumer = KafkaConsumer(
        TOPIC_ENTRADA,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="carga-group"
    )
    cargados   = 0
    rechazados = 0
    try:
        for mensaje in consumer:
            evento = mensaje.value
            ok_redis    = cargar_redis(r, evento)
            ok_postgres = cargar_postgres(conn, evento)
            if ok_redis and ok_postgres:
                cargados += 1
                print(f"  ✔ Cargado [{cargados}]: {evento['tipo_evento']} | "
                      f"{evento['usuario_origen']} → {evento['usuario_destino']}")
            else:
                rechazados += 1
                print(f"  ✘ Fallo | evento_id: {evento['evento_id']}")
    except KeyboardInterrupt:
        print(f"\n[CARGA] Detenido. Cargados: {cargados} | Rechazados: {rechazados}")
    finally:
        consumer.close()
        conn.close()
        print("[CARGA] Conexiones cerradas.")

if __name__ == "__main__":
    main()