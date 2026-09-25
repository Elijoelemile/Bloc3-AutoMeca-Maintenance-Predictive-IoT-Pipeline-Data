"""Test fonctionnel — routage vers la quarantaine (quality/ + ext_load_streaming)
contre un vrai ClickHouse. Seul Object Storage est simule (hors perimetre
ClickHouse, deja couvert par les tests unitaires de kafka_to_clickhouse.py).
"""
from datetime import datetime, timezone
from unittest.mock import patch

from ext_load_streaming.kafka_to_clickhouse import process_batch
from ext_load_streaming.mqtt_to_kafka import SensorMessage


def test_process_batch_routes_correctly_against_real_clickhouse(ch_client):
    valide = SensorMessage(401, "2030-03-01T06:00:00", 176.2, 418.5, 113.0, 45.0)
    invalide = SensorMessage(402, "2030-03-01T06:00:00", 999.0, 418.5, 113.0, 45.0)

    with patch("ext_load_streaming.kafka_to_clickhouse.upload_file"):
        process_batch(
            [valide, invalide], ch_client,
            batch_timestamp=datetime(2030, 3, 1, tzinfo=timezone.utc),
        )

    valides = ch_client.query(
        "SELECT machine_id FROM automeca.telemetrie WHERE machine_id = 401"
    ).result_rows
    assert valides == [(401,)]

    quarantaine = ch_client.query(
        "SELECT machine_id, motif FROM automeca.telemetrie_quarantaine WHERE machine_id = 402"
    ).result_rows
    assert quarantaine == [(402, "volt_hors_plage")]
