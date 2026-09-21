"""Tests unitaires — streaming/mqtt_to_kafka.py

Aucune vraie connexion MQTT/Kafka : on teste le parsing et le
comportement du handler en isolation.
"""
import json
from unittest.mock import MagicMock

import pytest

from streaming.mqtt_to_kafka import (
    MalformedMessageError,
    SensorMessage,
    make_on_message,
    parse_sensor_message,
    relay_to_kafka,
)

VALID_PAYLOAD = json.dumps({
    "machine_id": 1,
    "datetime_mes": "2026-01-15T06:00:00",
    "volt": 176.21,
    "rotate": 418.50,
    "pressure": 113.07,
    "vibration": 45.08,
}).encode("utf-8")


def test_parse_sensor_message_valid_payload():
    message = parse_sensor_message(VALID_PAYLOAD)
    assert message == SensorMessage(
        machine_id=1, datetime_mes="2026-01-15T06:00:00",
        volt=176.21, rotate=418.50, pressure=113.07, vibration=45.08,
    )


def test_parse_sensor_message_invalid_json():
    with pytest.raises(MalformedMessageError):
        parse_sensor_message(b"{not valid json")


def test_parse_sensor_message_missing_field():
    payload = json.dumps({"machine_id": 1, "volt": 176.2}).encode("utf-8")
    with pytest.raises(MalformedMessageError):
        parse_sensor_message(payload)


def test_parse_sensor_message_wrong_type():
    payload = json.dumps({
        "machine_id": "pas-un-nombre", "datetime_mes": "x",
        "volt": 1, "rotate": 1, "pressure": 1, "vibration": 1,
    }).encode("utf-8")
    with pytest.raises(MalformedMessageError):
        parse_sensor_message(payload)


def test_relay_to_kafka_produces_with_machine_id_as_key():
    producer = MagicMock()
    message = SensorMessage(1, "2026-01-15T06:00:00", 176.2, 418.5, 113.0, 45.0)

    relay_to_kafka(producer, "automeca.telemetrie", message)

    producer.produce.assert_called_once()
    _, kwargs = producer.produce.call_args
    assert kwargs["key"] == "1"
    assert json.loads(kwargs["value"])["machine_id"] == 1
    producer.poll.assert_called_once_with(0)


def test_on_message_handler_ignores_malformed_payload_without_crashing():
    producer = MagicMock()
    on_message = make_on_message(producer, "automeca.telemetrie")
    mqtt_msg = MagicMock(payload=b"{bad json", topic="automeca/capteurs/1/mesure")

    on_message(None, None, mqtt_msg)  # ne doit pas lever d'exception

    producer.produce.assert_not_called()


def test_on_message_handler_relays_valid_payload():
    producer = MagicMock()
    on_message = make_on_message(producer, "automeca.telemetrie")
    mqtt_msg = MagicMock(payload=VALID_PAYLOAD, topic="automeca/capteurs/1/mesure")

    on_message(None, None, mqtt_msg)

    producer.produce.assert_called_once()
