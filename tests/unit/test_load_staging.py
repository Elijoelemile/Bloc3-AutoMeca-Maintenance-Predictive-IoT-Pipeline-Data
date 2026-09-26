"""Tests unitaires — extraction/load_staging.py

DB et Object Storage simules : on ne teste que la logique (mapping
fichier -> table, colonnes, gestion des lignes vides).
"""
from unittest.mock import MagicMock, patch

from extraction.load_staging import STAGING_TARGETS, load_file_to_staging


CSV_MACHINES = "machineID,model,age\n1,model3,18\n2,model1,5\n"


@patch("extraction.load_staging._read_object_as_rows")
def test_load_file_to_staging_inserts_expected_columns(mock_read):
    # Cles = vrais en-tetes du CSV Kaggle brut (machineID), PAS les noms de
    # colonnes staging cibles (machine_id) — sans cette distinction, ce test
    # ne peut pas detecter une regression sur le mapping source->cible (bug
    # reel trouve lors d'un audit : STAGING_TARGETS utilisait autrefois les
    # noms cibles pour indexer le CSV source, KeyError garanti en production).
    mock_read.return_value = [
        {"machineID": "1", "model": "model3", "age": "18"},
        {"machineID": "2", "model": "model1", "age": "5"},
    ]
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    with patch("extraction.load_staging.psycopg2.extras.execute_values") as mock_execute_values:
        count = load_file_to_staging("raw/gmao_erp/2026-01-15/PdM_machines.csv", "PdM_machines.csv", mock_conn)

    assert count == 2
    mock_execute_values.assert_called_once()
    _, args, _ = mock_execute_values.mock_calls[0]
    _, sql_text, values = args
    assert "staging.machines" in sql_text
    assert "ON CONFLICT DO NOTHING" in sql_text
    assert values == [("1", "model3", "18"), ("2", "model1", "5")]
    mock_conn.commit.assert_called_once()


@patch("extraction.load_staging._read_object_as_rows")
def test_load_file_to_staging_handles_empty_file(mock_read):
    mock_read.return_value = []
    mock_conn = MagicMock()

    count = load_file_to_staging("raw/gmao_erp/2026-01-15/PdM_machines.csv", "PdM_machines.csv", mock_conn)

    assert count == 0
    mock_conn.commit.assert_not_called()


def test_load_file_to_staging_rejects_unknown_file():
    try:
        load_file_to_staging("raw/x/y.csv", "unknown.csv", MagicMock())
        assert False, "devrait lever ValueError"
    except ValueError:
        pass


def test_staging_targets_cover_all_batch_tables():
    assert set(STAGING_TARGETS) == {
        "PdM_machines.csv", "PdM_errors.csv", "PdM_failures.csv", "PdM_maint.csv",
    }
