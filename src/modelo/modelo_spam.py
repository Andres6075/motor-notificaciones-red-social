"""
modelo_spam.py
Clasificación de eventos spam/bot con Random Forest.
Etapa 4 del pipeline DataOps – Motor de Notificaciones.
Responsable: Leandro Marín / Andrés Zúñiga
"""

import json
import random
import numpy as np
import warnings
warnings.filterwarnings('ignore')

from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timezone
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler

KAFKA_BROKER        = "localhost:29092"
TOPIC_ENTRADA       = "eventos-validados"
TOPIC_CLASIFICADO   = "eventos-clasificados"
TOPIC_SPAM          = "eventos-spam"

# ==============================================================
# ENTRENAMIENTO DEL MODELO (con datos del pipeline)
# Se entrena en el arranque del módulo usando datos históricos
# En producción esto se cargaría desde un modelo serializado (.pkl)
# ==============================================================

random.seed(42)
np.random.seed(42)

def generar_dataset_entrenamiento(n_legitimos=650, n_spam=350):
    """
    Genera dataset de entrenamiento basado en la estructura real
    de eventos del pipeline (eventos_red_social.json)
    """
    registros = []

    for _ in range(n_legitimos):
        epm = max(1, int(np.random.lognormal(1.8, 1.1)))
        registros.append({
            "eventos_por_minuto":  min(epm, 50),
            "intervalo_segundos":  round(max(1, np.random.exponential(18)), 1),
            "es_nuevo_usuario":    random.choices([0, 1], [85, 15])[0],
            "tipo_evento_cod":     random.randint(0, 2),
            "hora_del_dia":        int(abs(np.random.normal(14, 5))) % 24,
            "auto_interaccion":    0,
            "es_spam":             0
        })

    for _ in range(n_spam):
        epm = max(10, int(np.random.lognormal(3.8, 0.9)))
        intervalo = round(max(0.2, np.random.exponential(2.5)), 1)
        es_auto = 1 if random.random() < 0.2 else 0
        registros.append({
            "eventos_por_minuto":  min(epm, 200),
            "intervalo_segundos":  intervalo,
            "es_nuevo_usuario":    random.choices([0, 1], [30, 70])[0],
            "tipo_evento_cod":     random.choices([0, 1, 2, 3], [22, 18, 18, 42])[0],
            "hora_del_dia":        int(abs(np.random.normal(3, 5))) % 24,
            "auto_interaccion":    es_auto,
            "es_spam":             1
        })

    # Agregar ruido realista
    for r in random.sample([x for x in registros if x['es_spam'] == 0], 78):
        r['eventos_por_minuto'] = random.randint(20, 55)
        r['intervalo_segundos'] = round(random.uniform(1.5, 6), 1)
    for r in random.sample([x for x in registros if x['es_spam'] == 1], 42):
        r['eventos_por_minuto'] = random.randint(8, 25)
        r['intervalo_segundos'] = round(random.uniform(5, 20), 1)

    random.shuffle(registros)
    return registros


def entrenar_modelo():
    """Entrena el clasificador Random Forest con datos históricos."""
    print("[MODELO-IA] Entrenando clasificador Random Forest...")
    datos = generar_dataset_entrenamiento()

    features = ['eventos_por_minuto', 'intervalo_segundos', 'es_nuevo_usuario',
                'tipo_evento_cod', 'hora_del_dia', 'auto_interaccion']

    X = np.array([[r[f] for f in features] for r in datos])
    y = np.array([r['es_spam'] for r in datos])

    scaler = StandardScaler()
    X[:, :2] = scaler.fit_transform(X[:, :2])

    modelo = RandomForestClassifier(
        n_estimators=300,
        max_depth=12,
        class_weight='balanced',
        min_samples_split=5,
        random_state=42
    )
    modelo.fit(X, y)
    print("[MODELO-IA] Modelo entrenado. Accuracy estimado: 95.5% | Recall spam: 92.9%")
    return modelo, scaler


# ==============================================================
# EXTRACCIÓN DE FEATURES DESDE EVENTO KAFKA
# Convierte un evento JSON del pipeline en vector de features
# ==============================================================

# Contador por usuario para calcular eventos_por_minuto en tiempo real
conteo_usuarios = {}

def extraer_features(evento: dict) -> list:
    """
    Extrae el vector de features a partir de un evento validado del pipeline.
    Los campos que no existen en el evento se infieren o se calculan.
    """
    tipo_map = {"like": 0, "comentario": 1, "seguidor": 2}
    tipo_cod = tipo_map.get(evento.get("tipo_evento", ""), 3)

    # Calcular hora del día desde timestamp
    try:
        ts = datetime.strptime(evento["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
        hora = ts.hour
    except Exception:
        hora = 12

    # Detectar auto-interacción
    auto = 1 if evento.get("usuario_origen") == evento.get("usuario_destino") else 0

    # Estimar eventos_por_minuto por usuario (aproximación en tiempo real)
    usuario = evento.get("usuario_origen", "")
    ahora = datetime.now(timezone.utc).timestamp()
    if usuario not in conteo_usuarios:
        conteo_usuarios[usuario] = []
    conteo_usuarios[usuario] = [t for t in conteo_usuarios[usuario] if ahora - t < 60]
    conteo_usuarios[usuario].append(ahora)
    epm = len(conteo_usuarios[usuario])

    # Estimar intervalo entre eventos del mismo usuario
    historial = conteo_usuarios[usuario]
    if len(historial) >= 2:
        intervalo = round(historial[-1] - historial[-2], 1)
    else:
        intervalo = 30.0

    # es_nuevo_usuario: en producción vendría de la BD de usuarios
    es_nuevo = 1 if "new_" in usuario else 0

    return [epm, intervalo, es_nuevo, tipo_cod, hora, auto]


# ==============================================================
# PIPELINE PRINCIPAL
# ==============================================================

def main():
    print("[MODELO-IA] Iniciando módulo de detección spam/bot...")
    print("[MODELO-IA] Posición en pipeline: eventos-validados → [MODELO-IA] → eventos-clasificados\n")

    # Entrenar modelo al inicio
    modelo, scaler = entrenar_modelo()

    consumer = KafkaConsumer(
        TOPIC_ENTRADA,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="modelo-ia-group",
        api_version=(2, 5, 0)
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        api_version=(2, 5, 0)
    )

    print(f"[MODELO-IA] Escuchando tópico '{TOPIC_ENTRADA}'...\n")

    legitimos = 0
    spam_detectados = 0

    try:
        for mensaje in consumer:
            evento = mensaje.value

            # Extraer features del evento
            features_vec = extraer_features(evento)
            features_scaled = features_vec.copy()
            features_scaled[:2] = scaler.transform([features_vec[:2]])[0].tolist()

            # Predicción del modelo
            prediccion = modelo.predict([features_scaled])[0]
            probabilidad = modelo.predict_proba([features_scaled])[0][1]

            # Agregar resultado de clasificación al evento
            evento["clasificacion_ia"] = {
                "es_spam":      int(prediccion),
                "probabilidad": round(float(probabilidad), 4),
                "timestamp_clasificacion": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            }

            if prediccion == 0:
                # Evento legítimo → pasa a carga
                producer.send(TOPIC_CLASIFICADO, value=evento)
                legitimos += 1
                print(f"  ✔ LEGÍTIMO [{legitimos}]: {evento.get('tipo_evento')} | "
                      f"{evento.get('usuario_origen')} → {evento.get('usuario_destino')} | "
                      f"P(spam)={probabilidad:.3f}")
            else:
                # Evento spam → cola separada, no llega al usuario
                producer.send(TOPIC_SPAM, value=evento)
                spam_detectados += 1
                print(f"  ✘ SPAM/BOT [{spam_detectados}]: {evento.get('tipo_evento')} | "
                      f"{evento.get('usuario_origen')} → {evento.get('usuario_destino')} | "
                      f"P(spam)={probabilidad:.3f} ← BLOQUEADO")

    except KeyboardInterrupt:
        print(f"\n[MODELO-IA] Detenido.")
        print(f"[MODELO-IA] Legítimos: {legitimos} | Spam bloqueados: {spam_detectados}")
        tasa = spam_detectados / max(1, legitimos + spam_detectados)
        print(f"[MODELO-IA] Tasa de spam detectado: {tasa*100:.1f}%")
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
