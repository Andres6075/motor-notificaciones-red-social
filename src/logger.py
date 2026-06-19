"""
logger.py
Logger centralizado con formato JSON estructurado.
Todos los módulos del pipeline importan este logger para generar
logs uniformes con timestamp ISO 8601, nivel, servicio y métricas.
"""

import logging
import json
import time
import os
from datetime import datetime, timezone


class JSONFormatter(logging.Formatter):
    """Convierte cada registro de log a JSON estructurado."""

    def format(self, record):
        entrada = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "nivel":     record.levelname,
            "servicio":  record.name,
            "mensaje":   record.getMessage(),
        }
        # Agregar métricas extra si el módulo las envía (latencia, throughput, etc.)
        if hasattr(record, "extra"):
            entrada.update(record.extra)
        return json.dumps(entrada, ensure_ascii=False)


def get_logger(nombre: str) -> logging.Logger:
    """
    Retorna un logger con salida JSON a archivo (logs/pipeline.log) y consola.

    Args:
        nombre: Nombre del módulo, ej: 'ingesta', 'limpieza', 'modelo_ia'
    Returns:
        Logger configurado
    """
    logger = logging.getLogger(nombre)
    if logger.handlers:
        return logger  # ya estaba configurado

    logger.setLevel(logging.DEBUG)
    os.makedirs("logs", exist_ok=True)

    # Archivo: guarda todos los niveles
    fh = logging.FileHandler("logs/pipeline.log", encoding="utf-8")
    fh.setFormatter(JSONFormatter())
    fh.setLevel(logging.DEBUG)

    # Consola: muestra INFO en adelante
    ch = logging.StreamHandler()
    ch.setFormatter(JSONFormatter())
    ch.setLevel(logging.INFO)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


class PipelineMetrics:
    """
    Registra y loggea métricas de rendimiento de cada módulo del pipeline.
    Mide latencia, throughput, errores y genera resumen final.
    """

    def __init__(self, servicio: str):
        self.logger    = get_logger(servicio)
        self.servicio  = servicio
        self.procesados = 0
        self.errores    = 0
        self.inicio     = time.time()

    def ok(self, latencia_ms: float, detalle: dict = None):
        """Registra un evento procesado correctamente."""
        self.procesados += 1
        extra = {"latencia_ms": round(latencia_ms, 2), "total_procesados": self.procesados}
        if detalle:
            extra.update(detalle)
        self.logger.info("Evento procesado OK", extra={"extra": extra})

    def error(self, mensaje: str, detalle: dict = None):
        """Registra un error de procesamiento."""
        self.errores += 1
        extra = {"errores_acumulados": self.errores}
        if detalle:
            extra.update(detalle)
        self.logger.error(mensaje, extra={"extra": extra})

    def warning(self, mensaje: str, detalle: dict = None):
        """Registra una advertencia (duplicado, descartado, etc.)."""
        extra = detalle or {}
        self.logger.warning(mensaje, extra={"extra": extra})

    def resumen(self):
        """Loggea resumen final con KPIs del módulo."""
        duracion   = time.time() - self.inicio
        throughput = self.procesados / duracion if duracion > 0 else 0
        tasa_error = (self.errores / max(self.procesados, 1)) * 100
        self.logger.info("Resumen de ejecución", extra={"extra": {
            "duracion_seg":       round(duracion, 2),
            "total_procesados":   self.procesados,
            "total_errores":      self.errores,
            "throughput_ev_seg":  round(throughput, 2),
            "tasa_error_pct":     round(tasa_error, 4),
        }})