"""Tests unitaires — transform/consolidation.py

ClickHouse et PostgreSQL mockes : on teste la logique de fusion (grille
dense sur toutes les machines connues) et le SQL d'ecriture (upsert),
sans connexion reelle.
"""
from datetime import date
from unittest.mock import MagicMock, patch

from transform.consolidation import (
    build_consolidation_rows,
    fetch_evenements_jour,
    fetch_machine_id_map,
    fetch_telemetrie_agregats,
    upsert_consolidation,
)

RUN_DATE = date(2026, 1, 15)
ID_DATE = 20260115


def test_build_consolidation_rows_dense_grid_all_known_machines():
    telemetrie = {1: {"rms_vibration_moyen": 35.3, "rms_vibration_max": 50.0, "tendance_pression_moyenne": 5.0}}
    evenements = {2: {"nb_erreurs": 1, "nb_pannes": 0, "nb_maintenances": 0}}
    machine_id_map = {1: 101, 2: 102, 3: 103}  # machine_id_nat -> id_machine

    rows = build_consolidation_rows(telemetrie, evenements, machine_id_map, RUN_DATE)

    assert len(rows) == 3  # une ligne par machine connue, meme sans donnee (machine 3)
    rows_by_id_machine = {r[1]: r for r in rows}

    machine_1 = rows_by_id_machine[101]
    assert machine_1 == (ID_DATE, 101, 35.3, 50.0, 5.0, 0, 0, 0)

    machine_2 = rows_by_id_machine[102]
    assert machine_2 == (ID_DATE, 102, None, None, None, 1, 0, 0)

    machine_3_sans_donnee = rows_by_id_machine[103]
    assert machine_3_sans_donnee == (ID_DATE, 103, None, None, None, 0, 0, 0)


def test_fetch_telemetrie_agregats_builds_dict_keyed_by_machine_id():
    clickhouse_client = MagicMock()
    clickhouse_client.query.return_value.result_rows = [
        (1, 35.3, 50.0, 5.0),
        (2, 20.0, 25.0, -3.0),
    ]

    result = fetch_telemetrie_agregats(clickhouse_client, RUN_DATE)

    assert result == {
        1: {"rms_vibration_moyen": 35.3, "rms_vibration_max": 50.0, "tendance_pression_moyenne": 5.0},
        2: {"rms_vibration_moyen": 20.0, "rms_vibration_max": 25.0, "tendance_pression_moyenne": -3.0},
    }
    clickhouse_client.query.assert_called_once()
    _, kwargs = clickhouse_client.query.call_args
    assert kwargs["parameters"] == {"run_date": RUN_DATE}


def test_fetch_evenements_jour_builds_dict_keyed_by_machine_id_nat():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [(1, 2, 1, 0), (2, 0, 0, 1)]

    result = fetch_evenements_jour(mock_conn, RUN_DATE)

    assert result == {
        1: {"nb_erreurs": 2, "nb_pannes": 1, "nb_maintenances": 0},
        2: {"nb_erreurs": 0, "nb_pannes": 0, "nb_maintenances": 1},
    }
    called_sql, called_params = mock_cursor.execute.call_args[0]
    assert called_params == {"id_date": ID_DATE}


def test_fetch_machine_id_map():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [(1, 101), (2, 102)]

    result = fetch_machine_id_map(mock_conn)

    assert result == {1: 101, 2: 102}


def test_upsert_consolidation_uses_on_conflict_update():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    rows = [(ID_DATE, 101, 35.3, 50.0, 5.0, 0, 0, 0)]

    with patch("transform.consolidation.psycopg2.extras.execute_values") as mock_execute_values:
        upsert_consolidation(mock_conn, rows)

    mock_execute_values.assert_called_once()
    _, args, _ = mock_execute_values.mock_calls[0]
    _, sql_text, values = args
    assert "datamart.fait_telemetrie_jour" in sql_text
    assert "ON CONFLICT (id_date, id_machine) DO UPDATE" in sql_text
    assert values == rows
    mock_conn.commit.assert_called_once()


def test_upsert_consolidation_skips_empty_rows():
    mock_conn = MagicMock()

    upsert_consolidation(mock_conn, [])

    mock_conn.cursor.assert_not_called()
    mock_conn.commit.assert_not_called()
