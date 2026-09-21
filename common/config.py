"""Chargement de la configuration depuis les variables d'environnement.

Aucun secret en dur dans le code (voir .env.example pour la liste des
variables attendues). En local, un fichier .env (non versionne) est
charge automatiquement si python-dotenv est installe ; en production,
les variables sont injectees par l'orchestrateur (Airflow).
"""
import os
from dataclasses import dataclass

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


class MissingConfigError(RuntimeError):
    """Levee quand une variable d'environnement requise est absente."""


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise MissingConfigError(
            f"Variable d'environnement manquante : {name} "
            f"(voir .env.example)"
        )
    return value


def _optional_float(name: str, default: float) -> float:
    value = os.environ.get(name)
    return float(value) if value else default


@dataclass(frozen=True)
class KaggleConfig:
    username: str
    key: str


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str
    topic_telemetrie: str


@dataclass(frozen=True)
class MqttConfig:
    host: str
    port: int
    topic: str


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    database: str
    user: str
    password: str


@dataclass(frozen=True)
class ClickHouseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str


@dataclass(frozen=True)
class ObjectStorageConfig:
    endpoint: str
    bucket: str
    access_key: str
    secret_key: str


@dataclass(frozen=True)
class AlertThresholds:
    rms_vibration: float
    pressure_trend: float


def load_kaggle_config() -> KaggleConfig:
    return KaggleConfig(
        username=_require("KAGGLE_USERNAME"),
        key=_require("KAGGLE_KEY"),
    )


def load_kafka_config() -> KafkaConfig:
    return KafkaConfig(
        bootstrap_servers=_require("KAFKA_BOOTSTRAP_SERVERS"),
        topic_telemetrie=os.environ.get("KAFKA_TOPIC_TELEMETRIE", "automeca.telemetrie"),
    )


def load_mqtt_config() -> MqttConfig:
    return MqttConfig(
        host=_require("MQTT_BROKER_HOST"),
        port=int(os.environ.get("MQTT_BROKER_PORT", "8883")),
        topic=os.environ.get("MQTT_TOPIC", "automeca/capteurs/+/mesure"),
    )


def load_postgres_config() -> PostgresConfig:
    return PostgresConfig(
        host=_require("POSTGRES_HOST"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        database=os.environ.get("POSTGRES_DB", "automeca"),
        user=_require("POSTGRES_USER"),
        password=_require("POSTGRES_PASSWORD"),
    )


def load_clickhouse_config() -> ClickHouseConfig:
    return ClickHouseConfig(
        host=_require("CLICKHOUSE_HOST"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8443")),
        database=os.environ.get("CLICKHOUSE_DB", "automeca"),
        user=_require("CLICKHOUSE_USER"),
        password=_require("CLICKHOUSE_PASSWORD"),
    )


def load_object_storage_config() -> ObjectStorageConfig:
    return ObjectStorageConfig(
        endpoint=_require("OBJECT_STORAGE_ENDPOINT"),
        bucket=os.environ.get("OBJECT_STORAGE_BUCKET", "automeca-datalake"),
        access_key=_require("OBJECT_STORAGE_ACCESS_KEY"),
        secret_key=_require("OBJECT_STORAGE_SECRET_KEY"),
    )


def load_alert_thresholds() -> AlertThresholds:
    return AlertThresholds(
        rms_vibration=_optional_float("ALERT_RMS_VIBRATION_THRESHOLD", 45.0),
        pressure_trend=_optional_float("ALERT_PRESSURE_TREND_THRESHOLD", 15.0),
    )
