"""
db_loader.py (Versión de Presentación Ágil)
Carga rápida de eventos a Redis y PostgreSQL.
Etapa 4 del pipeline DataOps – Motor de Notificaciones.
Responsable: Andrés Zúñiga
"""

import json
import time
import redis
import psycopg2

# Configuración de conexiones locales (Docker)
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_TTL  = 300

PG_HOST = "localhost"
PG_PORT = 5432
PG_DB   = "notificaciones_db"
PG_USER = "pipeline_user"
PG_PASS = "pipeline_pass"

def conectar_redis():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping()
    print("[CARGA] Redis conectado exitosamente (Caché de alta velocidad).")
    return r

def conectar_postgres():
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    conn.autocommit = True
    print("[CARGA] PostgreSQL conectado exitosamente (Histórico relacional).")
    return conn

def crear_tabla(conn):
    with conn.cursor() as cur:
        # Reiniciamos la tabla vieja para aplicar la estructura de presentación sin conflictos
        cur.execute("DROP TABLE IF EXISTS eventos_procesados;")
        
        # Estructura limpia: Postgres maneja el timestamp de manera automática con DEFAULT NOW()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS eventos_procesados (
                evento_id        VARCHAR(50) PRIMARY KEY,
                tipo_evento      VARCHAR(20) NOT NULL,
                usuario_origen   VARCHAR(20) NOT NULL,
                usuario_destino  VARCHAR(20) NOT NULL,
                timestamp_evento TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """)
    print("[CARGA] Tabla 'eventos_procesados' verificada en la base de datos.")

def main():
    print("[CARGA] Iniciando capa de persistencia políglota...\n")
    try:
        # Inicialización de las conexiones
        r = conectar_redis()
        conn = conectar_postgres()
        crear_tabla(conn)
        
        # MEJORA PARCIAL 3: ahora lee desde 'eventos-clasificados'
        # Solo llegan eventos que el modelo IA aprobó como legítimos
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            "eventos-clasificados",
            bootstrap_servers="localhost:29092",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            group_id="carga-group",
            api_version=(2, 5, 0),
            consumer_timeout_ms=8000
        )

        print("\n[CARGA] Escuchando tópico 'eventos-clasificados' (eventos aprobados por Modelo IA)...")
        cur = conn.cursor()
        idx = 0
        for mensaje in consumer:
            evento = mensaje.value
            idx += 1
            clasificacion = evento.get("clasificacion_ia", {})
            prob_spam = clasificacion.get("probabilidad", 0.0)

            # 1. Almacenamiento rápido en Caché (Redis)
            clave_redis = f"notif:{evento['usuario_destino']}:{evento['evento_id']}"
            r.setex(clave_redis, REDIS_TTL, json.dumps(evento))

            # 2. Persistencia relacional histórica (PostgreSQL)
            cur.execute("""
                INSERT INTO eventos_procesados (evento_id, tipo_evento, usuario_origen, usuario_destino)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (evento_id) DO NOTHING;
            """, (evento['evento_id'], evento['tipo_evento'],
                  evento['usuario_origen'], evento['usuario_destino']))

            print(f"  ✔ Cargado [{idx}]: {evento['tipo_evento']} | "
                  f"{evento['usuario_origen']} → {evento['usuario_destino']} | "
                  f"P(spam)={prob_spam:.3f} ➔ Redis & Postgres")
            time.sleep(0.3)

        cur.close()
        consumer.close()
        conn.close()
        print(f"\n[CARGA] Pipeline completado. {idx} eventos almacenados (spam bloqueado por Modelo IA).")
        
    except Exception as e:
        print(f"\n[ERROR EN CARGA] No se pudo completar la persistencia: {e}")

if __name__ == "__main__":
    main()
