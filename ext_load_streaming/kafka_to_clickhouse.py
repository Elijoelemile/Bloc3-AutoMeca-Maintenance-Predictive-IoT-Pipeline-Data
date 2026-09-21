"""Consommateur Kafka -> data lake (Parquet) -> ClickHouse (Load, page 1).

Lit en continu les mesures deposees dans Kafka par mqtt_to_kafka.py et
les charge, par lots, aux deux destinations du couloir temps reel :
Object Storage (data lake, format Parquet, coherent avec les autres
sources du Bloc 2) puis la table brute automeca.telemetrie de
ClickHouse. Aucune transformation ici (voir la distinction Load/
Transform etablie pour ce projet) : la vue materialisee ClickHouse
(transform/materialized_view_telemetrie.sql, a venir) calcule ensuite
RMS et tendance a partir de cette table brute, apres coup.

Traitement par lots, pas message par message : un fichier Parquet par
message serait inutilisable pour un data lake, et une insertion
ClickHouse par message serait inefficace (ClickHouse est concu pour
des inserts en lot).

Livraison au moins une fois : l'offset Kafka n'est commite qu'apres
succes des deux ecritures (Object Storage puis ClickHouse). En cas de
crash entre les deux, le lot peut etre rejoue et donc duplique — la
table brute MergeTree du Bloc 2 ne deduplique pas. Acceptable pour ce
POC ; une exactement-une-fois necessiterait une cle de deduplication
(ReplacingMergeTree) hors perimetre de cette iteration.

Avant chargement, chaque lot passe par la regle de qualite "valeur hors
plage" (quality/validation_rules.py, page 2 du diagramme) : les mesures
suspectes sont isolees dans automeca.telemetrie_quarantaine plutot que
chargees normalement — jamais fusionnees avec les donnees validees.
"""
import io
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import clickhouse_connect
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer

from common.config import ClickHouseConfig, KafkaConfig, load_clickhouse_config, load_kafka_config
from common.logging_config import get_logger
from common.object_storage import upload_file
from ext_load_streaming.mqtt_to_kafka import MalformedMessageError, SensorMessage, parse_sensor_message
from quality.validation_rules import partition_messages

logger = get_logger(__name__)

CONSUMER_GROUP_ID = "bloc3-kafka-to-clickhouse"
BATCH_SIZE = 500
BATCH_INTERVAL_SECONDS = 10.0
CLICKHOUSE_TABLE = "automeca.telemetrie"
CLICKHOUSE_COLUMNS = ["machine_id", "datetime_mes", "volt", "rotate", "pressure", "vibration"]
QUARANTINE_TABLE = "automeca.telemetrie_quarantaine"
QUARANTINE_COLUMNS = CLICKHOUSE_COLUMNS + ["motif"]


def parse_kafka_message(payload: bytes) -> SensorMessage:
    """Meme structure que le message MQTT d'origine (voir relay_to_kafka)."""
    return parse_sensor_message(payload)


def object_key_for_batch(batch_timestamp: datetime) -> str:
    date_part = batch_timestamp.date().isoformat()
    horaire = batch_timestamp.strftime("%H%M%S")
    return f"raw/telemetrie/{date_part}/{horaire}-{uuid4().hex[:8]}.parquet"


def messages_to_parquet_bytes(messages: list[SensorMessage]) -> bytes:
    table = pa.table({
        "machine_id": [m.machine_id for m in messages],
        "datetime_mes": [m.datetime_mes for m in messages],
        "volt": [m.volt for m in messages],
        "rotate": [m.rotate for m in messages],
        "pressure": [m.pressure for m in messages],
        "vibration": [m.vibration for m in messages],
    })
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue()


def build_clickhouse_rows(messages: list[SensorMessage]) -> list[tuple]:
    # datetime_mes est une chaine ISO (issue du JSON Kafka) : ClickHouse
    # (colonne DateTime) exige un vrai objet datetime, pas une chaine.
    return [
        (m.machine_id, datetime.fromisoformat(m.datetime_mes), m.volt, m.rotate, m.pressure, m.vibration)
        for m in messages
    ]


def flush_batch(messages: list[SensorMessage], clickhouse_client, batch_timestamp: datetime | None = None) -> str:
    """Depose le lot dans le data lake (Parquet) puis la table brute ClickHouse.

    Retourne la cle objet deposee (pour tracabilite/log).
    """
    batch_timestamp = batch_timestamp or datetime.now(timezone.utc)
    object_key = object_key_for_batch(batch_timestamp)

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
        tmp_file.write(messages_to_parquet_bytes(messages))
        tmp_path = tmp_file.name
    try:
        upload_file(tmp_path, object_key)
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    clickhouse_client.insert(CLICKHOUSE_TABLE, build_clickhouse_rows(messages), column_names=CLICKHOUSE_COLUMNS)
    logger.info("Lot charge : %d mesures -> %s + table %s", len(messages), object_key, CLICKHOUSE_TABLE)
    return object_key


def flush_quarantine(quarantined: list[tuple[SensorMessage, str]], clickhouse_client) -> None:
    """Isole les mesures rejetees dans automeca.telemetrie_quarantaine — jamais fusionnees avec la table brute."""
    if not quarantined:
        return
    rows = [
        (m.machine_id, datetime.fromisoformat(m.datetime_mes), m.volt, m.rotate, m.pressure, m.vibration, motif)
        for m, motif in quarantined
    ]
    clickhouse_client.insert(QUARANTINE_TABLE, rows, column_names=QUARANTINE_COLUMNS)
    logger.warning("Lot mis en quarantaine : %d mesures", len(rows))


def process_batch(messages: list[SensorMessage], clickhouse_client, batch_timestamp: datetime | None = None) -> str | None:
    """Applique la regle "valeur hors plage" (Validation & Quarantaine, page 2)
    avant chargement. Retourne la cle objet du lot valide depose, ou None si
    le lot entier a ete mis en quarantaine."""
    valides, quarantaine = partition_messages(messages)
    flush_quarantine(quarantaine, clickhouse_client)
    if not valides:
        return None
    return flush_batch(valides, clickhouse_client, batch_timestamp=batch_timestamp)


def get_clickhouse_client(config: ClickHouseConfig | None = None):
    cfg = config or load_clickhouse_config()
    return clickhouse_connect.get_client(
        host=cfg.host, port=cfg.port, database=cfg.database,
        username=cfg.user, password=cfg.password,
    )


def run(kafka_config: KafkaConfig | None = None) -> None:
    kafka_cfg = kafka_config or load_kafka_config()
    clickhouse_client = get_clickhouse_client()

    consumer = Consumer({
        "bootstrap.servers": kafka_cfg.bootstrap_servers,
        "group.id": CONSUMER_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([kafka_cfg.topic_telemetrie])
    logger.info("Consommateur Kafka -> ClickHouse demarre, abonne a %s", kafka_cfg.topic_telemetrie)

    buffer: list[SensorMessage] = []
    last_flush = datetime.now(timezone.utc)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            now = datetime.now(timezone.utc)

            if msg is not None:
                if msg.error():
                    logger.error("Erreur consommateur Kafka : %s", msg.error())
                else:
                    try:
                        buffer.append(parse_kafka_message(msg.value()))
                    except MalformedMessageError:
                        logger.warning("Message Kafka rejete (structure invalide), offset=%s", msg.offset())

            elapsed = (now - last_flush).total_seconds()
            if buffer and (len(buffer) >= BATCH_SIZE or elapsed >= BATCH_INTERVAL_SECONDS):
                process_batch(buffer, clickhouse_client, batch_timestamp=now)
                consumer.commit(asynchronous=False)
                buffer = []
                last_flush = now
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
