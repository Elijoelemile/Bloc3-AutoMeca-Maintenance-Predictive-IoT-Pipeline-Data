"""Tests unitaires — quality/validation_rules.py

Fonctions pures (aucune simulation necessaire) + fonctions de cablage
(ClickHouse/PostgreSQL simules).
"""
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from ext_load_streaming.mqtt_to_kafka import SensorMessage
from quality.validation_rules import (
    detect_capteurs_silencieux,
    fetch_dernier_message_par_machine,
    fetch_machines_connues,
    partition_messages,
    valeur_hors_plage,
    verifier_capteurs_silencieux,
)

MESSAGE_VALIDE = SensorMessage(1, "2026-01-15T06:00:00", 176.2, 418.5, 113.0, 45.0)


def test_valeur_hors_plage_returns_none_for_plausible_measurement():
    assert valeur_hors_plage(MESSAGE_VALIDE) is None


def test_valeur_hors_plage_detects_volt_out_of_range():
    message = SensorMessage(1, "2026-01-15T06:00:00", 999.0, 418.5, 113.0, 45.0)
    assert valeur_hors_plage(message) == "volt_hors_plage"


def test_valeur_hors_plage_detects_negative_pressure():
    message = SensorMessage(1, "2026-01-15T06:00:00", 176.2, 418.5, -10.0, 45.0)
    assert valeur_hors_plage(message) == "pressure_hors_plage"


def test_valeur_hors_plage_never_rejects_real_dataset_extremes():
    # bornes min/max reellement observees dans PdM_telemetry.csv (876 100 lignes)
    extremes = SensorMessage(1, "2026-01-15T06:00:00", 97.33, 138.43, 51.24, 14.88)
    assert valeur_hors_plage(extremes) is None
    extremes_max = SensorMessage(1, "2026-01-15T06:00:00", 255.12, 695.02, 185.95, 76.79)
    assert valeur_hors_plage(extremes_max) is None


def test_partition_messages_splits_valid_and_quarantined():
    message_invalide = SensorMessage(2, "2026-01-15T06:00:00", 176.2, 418.5, -10.0, 45.0)

    valides, quarantaine = partition_messages([MESSAGE_VALIDE, message_invalide])

    assert valides == [MESSAGE_VALIDE]
    assert quarantaine == [(message_invalide, "pressure_hors_plage")]


def test_detect_capteurs_silencieux_flags_stale_and_never_seen_machines():
    reference_time = datetime(2026, 1, 15, 12, 0, 0)
    dernier_message = {
        1: reference_time - timedelta(minutes=1),   # recent, ok
        2: reference_time - timedelta(minutes=10),  # trop ancien
        # machine 3 : jamais vue
    }

    result = detect_capteurs_silencieux(
        dernier_message, machines_connues={1, 2, 3}, reference_time=reference_time,
        seuil=timedelta(minutes=5),
    )

    assert result == [2, 3]


def test_detect_capteurs_silencieux_empty_when_all_recent():
    reference_time = datetime(2026, 1, 15, 12, 0, 0)
    dernier_message = {1: reference_time - timedelta(minutes=1)}

    result = detect_capteurs_silencieux(
        dernier_message, machines_connues={1}, reference_time=reference_time,
        seuil=timedelta(minutes=5),
    )

    assert result == []


def test_fetch_dernier_message_par_machine_builds_dict():
    clickhouse_client = MagicMock()
    clickhouse_client.query.return_value.result_rows = [
        (1, datetime(2026, 1, 15, 11, 59)),
        (2, datetime(2026, 1, 15, 11, 50)),
    ]

    result = fetch_dernier_message_par_machine(clickhouse_client)

    assert result == {1: datetime(2026, 1, 15, 11, 59), 2: datetime(2026, 1, 15, 11, 50)}


def test_fetch_machines_connues_builds_set():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [(1,), (2,), (3,)]

    result = fetch_machines_connues(mock_conn)

    assert result == {1, 2, 3}


def test_verifier_capteurs_silencieux_end_to_end_with_mocks():
    reference_time = datetime(2026, 1, 15, 12, 0, tzinfo=timezone.utc)
    clickhouse_client = MagicMock()
    clickhouse_client.query.return_value.result_rows = [
        (1, reference_time - timedelta(minutes=1)),   # recent
        (2, reference_time - timedelta(minutes=10)),  # silencieuse
    ]
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [(1,), (2,), (3,)]  # machine 3 : jamais vue

    result = verifier_capteurs_silencieux(clickhouse_client, mock_conn, reference_time=reference_time)

    assert result == [2, 3]


def test_verifier_capteurs_silencieux_returns_empty_when_all_recent():
    reference_time = datetime(2026, 1, 15, 12, 0, tzinfo=timezone.utc)
    clickhouse_client = MagicMock()
    clickhouse_client.query.return_value.result_rows = [(1, reference_time - timedelta(minutes=1))]
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [(1,)]

    result = verifier_capteurs_silencieux(clickhouse_client, mock_conn, reference_time=reference_time)

    assert result == []
