"""Pont MQTT -> Kafka (flux temps reel, Extract, page 1 du diagramme).

Script maison (pas de connecteur Kafka Connect — choix valide pour sa
flexibilite, voir discussion du Bloc 3). Ecoute les mesures publiees
en MQTT par les capteurs et les republie telles quelles dans Kafka.
Aucune transformation, aucune validation metier ici (ce sera le role
de quality/validation_rules.py, en aval, sur les donnees deja dans
Kafka) — seulement une verification structurelle minimale, pour ne
jamais planter sur un message MQTT corrompu.

Processus persistant : tourne en continu, pas une tache Airflow
planifiee (voir bande "Orchestration" du diagramme).
"""
import json
from dataclasses import dataclass

from confluent_kafka import Producer
import paho.mqtt.client as mqtt

from common.config import KafkaConfig, MqttConfig, load_kafka_config, load_mqtt_config
from common.logging_config import get_logger

logger = get_logger(__name__)

REQUIRED_FIELDS = ("machine_id", "datetime_mes", "volt", "rotate", "pressure", "vibration")


class MalformedMessageError(ValueError):
    """Levee quand le payload MQTT n'a pas la structure attendue."""


@dataclass(frozen=True)
class SensorMessage:
    machine_id: int
    datetime_mes: str
    volt: float
    rotate: float
    pressure: float
    vibration: float


def parse_sensor_message(payload: bytes) -> SensorMessage:
    """Verifie la structure du message — pas la plausibilite des valeurs
    (ca, c'est le role de quality/validation_rules.py en aval)."""
    try:
        data = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise MalformedMessageError(f"JSON invalide : {exc}") from exc

    missing = [f for f in REQUIRED_FIELDS if f not in data]
    if missing:
        raise MalformedMessageError(f"Champs manquants : {missing}")

    try:
        return SensorMessage(
            machine_id=int(data["machine_id"]),
            datetime_mes=str(data["datetime_mes"]),
            volt=float(data["volt"]),
            rotate=float(data["rotate"]),
            pressure=float(data["pressure"]),
            vibration=float(data["vibration"]),
        )
    except (TypeError, ValueError) as exc:
        raise MalformedMessageError(f"Type de champ invalide : {exc}") from exc


def relay_to_kafka(producer: Producer, topic: str, message: SensorMessage) -> None:
    producer.produce(
        topic,
        key=str(message.machine_id),
        value=json.dumps(message.__dict__),
        callback=_delivery_callback,
    )
    producer.poll(0)


def _delivery_callback(err, msg) -> None:
    if err is not None:
        logger.error("Echec de publication Kafka : %s", err)


def make_on_message(producer: Producer, kafka_topic: str):
    def on_message(client, userdata, mqtt_msg) -> None:
        try:
            message = parse_sensor_message(mqtt_msg.payload)
        except MalformedMessageError:
            logger.warning("Message MQTT rejete (structure invalide) : %s", mqtt_msg.topic)
            return
        try:
            relay_to_kafka(producer, kafka_topic, message)
        except Exception:
            logger.exception("Echec de relais vers Kafka pour machine_id=%s", message.machine_id)
    return on_message


def run(mqtt_config: MqttConfig | None = None, kafka_config: KafkaConfig | None = None) -> None:
    mqtt_cfg = mqtt_config or load_mqtt_config()
    kafka_cfg = kafka_config or load_kafka_config()

    producer = Producer({"bootstrap.servers": kafka_cfg.bootstrap_servers})
    client = mqtt.Client()
    client.on_message = make_on_message(producer, kafka_cfg.topic_telemetrie)
    client.on_connect = lambda c, u, f, rc: logger.info("Connecte au broker MQTT (rc=%s)", rc)
    client.on_disconnect = lambda c, u, rc: logger.warning("Deconnecte du broker MQTT (rc=%s) — reconnexion automatique", rc)

    client.connect(mqtt_cfg.host, mqtt_cfg.port)
    client.subscribe(mqtt_cfg.topic)
    logger.info("Pont MQTT -> Kafka demarre, abonne a %s", mqtt_cfg.topic)
    client.loop_forever(retry_first_connection=True)


if __name__ == "__main__":
    run()
