"""Regles de qualite — flux temps reel (page 2 du diagramme, section
Validation & Quarantaine).

Deux controles de nature differente (le diagramme precise desormais la
distinction) :
- **Valeur hors plage** : controle PAR MESURE, applique a chaque message
  avant chargement (voir ext_load_streaming/kafka_to_clickhouse.py) —
  une mesure suspecte est isolee en quarantaine (ClickHouse, table
  creee par quality/00_add_quarantine_table.sql), jamais fusionnee avec
  les donnees validees.
- **Capteur silencieux** : controle PERIODIQUE, pas un filtre par
  message — il n'y a rien a mettre en quarantaine quand aucun message
  n'arrive (une absence n'est pas une donnee a isoler). Cablee par
  orchestration/dag_pipeline_bloc3.py (tache planifiee toutes les
  minutes) via verifier_capteurs_silencieux(), ci-dessous.

Bornes de plausibilite calculees sur les 876 100 mesures reelles du
dataset Kaggle (PdM_telemetry.csv) : min/max observes + marge de
securite, pour ne jamais rejeter une vraie valeur historique — la
regle vise une panne capteur/donnee corrompue, pas une condition de
fonctionnement rare mais reelle.
"""
from datetime import datetime, timedelta, timezone

from common.logging_config import get_logger
from ext_load_streaming.mqtt_to_kafka import SensorMessage

logger = get_logger(__name__)

# (borne_min, borne_max) — min/max reels observes, elargis d'une marge
# de securite (voir docstring du module).
PLAGES_PLAUSIBLES = {
    "volt": (50.0, 300.0),
    "rotate": (100.0, 750.0),
    "pressure": (30.0, 200.0),
    "vibration": (5.0, 90.0),
}

SEUIL_SILENCE_DEFAUT = timedelta(minutes=5)


def valeur_hors_plage(message: SensorMessage) -> str | None:
    """Retourne le motif de rejet (champ hors plage) ou None si la mesure est plausible."""
    for champ, (borne_min, borne_max) in PLAGES_PLAUSIBLES.items():
        valeur = getattr(message, champ)
        if valeur < borne_min or valeur > borne_max:
            return f"{champ}_hors_plage"
    return None


def partition_messages(
    messages: list[SensorMessage],
) -> tuple[list[SensorMessage], list[tuple[SensorMessage, str]]]:
    """Separe les messages valides de ceux a mettre en quarantaine (avec motif)."""
    valides: list[SensorMessage] = []
    quarantaine: list[tuple[SensorMessage, str]] = []
    for message in messages:
        motif = valeur_hors_plage(message)
        if motif is None:
            valides.append(message)
        else:
            quarantaine.append((message, motif))
    return valides, quarantaine


def detect_capteurs_silencieux(
    dernier_message_par_machine: dict[int, datetime],
    machines_connues: set[int],
    reference_time: datetime,
    seuil: timedelta = SEUIL_SILENCE_DEFAUT,
) -> list[int]:
    """Controle periodique : liste des machines dont la derniere mesure
    depasse le seuil de silence — y compris les machines connues n'ayant
    jamais emis (absentes du dict). Pas un filtre par message."""
    silencieuses = []
    for machine_id in machines_connues:
        dernier = dernier_message_par_machine.get(machine_id)
        if dernier is None or (reference_time - dernier) > seuil:
            silencieuses.append(machine_id)
    return sorted(silencieuses)


def fetch_dernier_message_par_machine(clickhouse_client) -> dict[int, datetime]:
    result = clickhouse_client.query(
        "SELECT machine_id, max(datetime_mes) FROM automeca.telemetrie GROUP BY machine_id"
    )
    return dict(result.result_rows)


def fetch_machines_connues(conn) -> set[int]:
    with conn.cursor() as cur:
        cur.execute("SELECT machine_id_nat FROM datamart.dim_machine")
        return {row[0] for row in cur.fetchall()}


def verifier_capteurs_silencieux(
    clickhouse_client,
    conn,
    reference_time: datetime | None = None,
    seuil: timedelta = SEUIL_SILENCE_DEFAUT,
) -> list[int]:
    """Cablage reel du controle periodique — appele par orchestration/
    (tache planifiee). Journalise une alerte si des machines sont
    silencieuses, ne leve jamais d'exception pour une liste vide."""
    reference_time = reference_time or datetime.now(timezone.utc)
    dernier_message = fetch_dernier_message_par_machine(clickhouse_client)
    machines_connues = fetch_machines_connues(conn)
    silencieuses = detect_capteurs_silencieux(dernier_message, machines_connues, reference_time, seuil)
    if silencieuses:
        logger.warning("Capteurs silencieux detectes (aucune mesure depuis %s) : %s", seuil, silencieuses)
    return silencieuses
