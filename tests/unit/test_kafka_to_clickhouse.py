"""Tests unitaires — ext_load_streaming/kafka_to_clickhouse.py

Aucune vraie connexion Kafka/ClickHouse/Object Storage : on teste les
fonctions pures (parsing, formatage Parquet, cle objet) et le lot
(flush_batch) avec Object Storage et ClickHouse mockes.
"""
import io
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pyarrow.parquet as pq
import pytest

from ext_load_streaming.kafka_to_clickhouse import (
    CLICKHOUSE_COLUMNS,
    CLICKHOUSE_TABLE,
    build_clickhouse_rows,
    flush_batch,
    messages_to_parquet_bytes,
    object_key_for_batch,
    parse_kafka_message,
)
from ext_load_streaming.mqtt_to_kafka import MalformedMessageError, SensorMessage

SAMPLE_MESSAGES = [
    SensorMessage(1, "2026-01-15T06:00:00", 176.2, 418.5, 113.0, 45.0),
    SensorMessage(2, "2026-01-15T06:00:00", 170.1, 400.0, 110.5, 40.2),
]


def test_parse_kafka_message_valid_payload():
    payload = json.dumps({
        "machine_id": 1, "datetime_mes": "2026-01-15T06:00:00",
        "volt": 176.2, "rotate": 418.5, "pressure": 113.0, "vibration": 45.0,
    }).encode("utf-8")

    message = parse_kafka_message(payload)

    assert message == SensorMessage(1, "2026-01-15T06:00:00", 176.2, 418.5, 113.0, 45.0)


def test_parse_kafka_message_malformed_raises():
    with pytest.raises(MalformedMessageError):
        parse_kafka_message(b"{not valid json")


def test_object_key_for_batch_format():
    batch_timestamp = datetime(2026, 1, 15, 6, 30, 45, tzinfo=timezone.utc)

    key = object_key_for_batch(batch_timestamp)

    assert key.startswith("raw/telemetrie/2026-01-15/063045-")
    assert key.endswith(".parquet")


def test_messages_to_parquet_bytes_roundtrip():
    raw = messages_to_parquet_bytes(SAMPLE_MESSAGES)

    table = pq.read_table(io.BytesIO(raw))
    assert table.num_rows == 2
    assert table.column("machine_id").to_pylist() == [1, 2]
    assert table.column("vibration").to_pylist() == [45.0, 40.2]


def test_build_clickhouse_rows_matches_column_order():
    rows = build_clickhouse_rows(SAMPLE_MESSAGES)

    assert rows == [
        (1, "2026-01-15T06:00:00", 176.2, 418.5, 113.0, 45.0),
        (2, "2026-01-15T06:00:00", 170.1, 400.0, 110.5, 40.2),
    ]


def test_flush_batch_uploads_to_datalake_and_inserts_clickhouse():
    clickhouse_client = MagicMock()
    batch_timestamp = datetime(2026, 1, 15, 6, 30, 45, tzinfo=timezone.utc)

    with patch("ext_load_streaming.kafka_to_clickhouse.upload_file") as mock_upload:
        object_key = flush_batch(SAMPLE_MESSAGES, clickhouse_client, batch_timestamp=batch_timestamp)

    mock_upload.assert_called_once()
    _, called_key = mock_upload.call_args[0]
    assert called_key == object_key
    assert object_key.startswith("raw/telemetrie/2026-01-15/")

    clickhouse_client.insert.assert_called_once()
    args, kwargs = clickhouse_client.insert.call_args
    assert args[0] == CLICKHOUSE_TABLE
    assert args[1] == build_clickhouse_rows(SAMPLE_MESSAGES)
    assert kwargs["column_names"] == CLICKHOUSE_COLUMNS
