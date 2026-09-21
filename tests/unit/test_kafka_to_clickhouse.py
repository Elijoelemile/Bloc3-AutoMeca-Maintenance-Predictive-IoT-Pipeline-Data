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
    QUARANTINE_COLUMNS,
    QUARANTINE_TABLE,
    build_clickhouse_rows,
    flush_batch,
    flush_quarantine,
    messages_to_parquet_bytes,
    object_key_for_batch,
    parse_kafka_message,
    process_batch,
    try_flush,
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
        (1, datetime(2026, 1, 15, 6, 0, 0), 176.2, 418.5, 113.0, 45.0),
        (2, datetime(2026, 1, 15, 6, 0, 0), 170.1, 400.0, 110.5, 40.2),
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


def test_flush_quarantine_inserts_with_motif_column():
    clickhouse_client = MagicMock()
    message = SensorMessage(3, "2026-01-15T06:00:00", 999.0, 418.5, 113.0, 45.0)

    flush_quarantine([(message, "volt_hors_plage")], clickhouse_client)

    clickhouse_client.insert.assert_called_once()
    args, kwargs = clickhouse_client.insert.call_args
    assert args[0] == QUARANTINE_TABLE
    assert args[1] == [(3, datetime(2026, 1, 15, 6, 0, 0), 999.0, 418.5, 113.0, 45.0, "volt_hors_plage")]
    assert kwargs["column_names"] == QUARANTINE_COLUMNS


def test_flush_quarantine_skips_empty():
    clickhouse_client = MagicMock()

    flush_quarantine([], clickhouse_client)

    clickhouse_client.insert.assert_not_called()


def test_process_batch_routes_valid_and_quarantined_separately():
    clickhouse_client = MagicMock()
    message_invalide = SensorMessage(3, "2026-01-15T06:00:00", 999.0, 418.5, 113.0, 45.0)
    batch_timestamp = datetime(2026, 1, 15, 6, 30, 45, tzinfo=timezone.utc)

    with patch("ext_load_streaming.kafka_to_clickhouse.upload_file"):
        object_key = process_batch(SAMPLE_MESSAGES + [message_invalide], clickhouse_client, batch_timestamp=batch_timestamp)

    assert object_key is not None
    # 2 inserts ClickHouse : un pour le lot valide (table brute), un pour la quarantaine
    assert clickhouse_client.insert.call_count == 2
    tables_called = {call.args[0] for call in clickhouse_client.insert.call_args_list}
    assert tables_called == {CLICKHOUSE_TABLE, QUARANTINE_TABLE}


def test_process_batch_returns_none_when_entire_batch_quarantined():
    clickhouse_client = MagicMock()
    message_invalide = SensorMessage(3, "2026-01-15T06:00:00", 999.0, 418.5, 113.0, 45.0)

    result = process_batch([message_invalide], clickhouse_client, batch_timestamp=datetime.now(timezone.utc))

    assert result is None
    clickhouse_client.insert.assert_called_once()
    assert clickhouse_client.insert.call_args.args[0] == QUARANTINE_TABLE


def test_try_flush_returns_true_on_success():
    clickhouse_client = MagicMock()
    batch_timestamp = datetime(2026, 1, 15, 6, 30, 45, tzinfo=timezone.utc)

    with patch("ext_load_streaming.kafka_to_clickhouse.upload_file"):
        result = try_flush(SAMPLE_MESSAGES, clickhouse_client, batch_timestamp)

    assert result is True


def test_try_flush_does_not_raise_and_returns_false_on_failure():
    clickhouse_client = MagicMock()
    clickhouse_client.insert.side_effect = ConnectionError("ClickHouse injoignable")
    batch_timestamp = datetime(2026, 1, 15, 6, 30, 45, tzinfo=timezone.utc)

    with patch("ext_load_streaming.kafka_to_clickhouse.upload_file"):
        result = try_flush(SAMPLE_MESSAGES, clickhouse_client, batch_timestamp)  # ne doit pas lever

    assert result is False
