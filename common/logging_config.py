"""Configuration de logs partagee par tous les composants du pipeline.

Chaque script (extraction, ext_load_streaming, orchestration...) appelle
get_logger(__name__) plutot que d'utiliser print() — necessaire pour
diagnostiquer un echec en production (voir page 2 du diagramme :
alerte operationnelle en cas d'echec).
"""
import logging
import os
import sys

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

_configured = False


def _configure_root() -> None:
    global _configured
    if _configured:
        return
    logging.basicConfig(
        level=LOG_LEVEL,
        format=LOG_FORMAT,
        stream=sys.stdout,
    )
    _configured = True


def get_logger(name: str) -> logging.Logger:
    """Retourne un logger configure pour le module appelant."""
    _configure_root()
    return logging.getLogger(name)
