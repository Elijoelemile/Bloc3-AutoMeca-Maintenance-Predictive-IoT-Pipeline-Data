"""Tests unitaires — orchestration/sql_runner.py

PostgreSQL simule : on verifie que le contenu du fichier est bien lu et
execute, et que la connexion est fermee meme en cas d'echec.
"""
from unittest.mock import MagicMock, patch

import pytest

from orchestration.sql_runner import executer_fichier_sql

FAKE_PG_CONFIG = MagicMock(host="h", port=5432, database="automeca", user="u", password="p")


def test_executer_fichier_sql_reads_and_executes_file_content(tmp_path):
    sql_file = tmp_path / "exemple.sql"
    sql_file.write_text("SELECT 1;", encoding="utf-8")
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    with patch("orchestration.sql_runner.psycopg2.connect", return_value=mock_conn) as mock_connect:
        executer_fichier_sql(str(sql_file), pg_config=FAKE_PG_CONFIG)

    mock_connect.assert_called_once_with(
        host="h", port=5432, dbname="automeca", user="u", password="p",
    )
    mock_cursor.execute.assert_called_once_with("SELECT 1;")
    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()


def test_executer_fichier_sql_closes_connection_even_on_error(tmp_path):
    sql_file = tmp_path / "exemple.sql"
    sql_file.write_text("SELECT 1;", encoding="utf-8")
    mock_conn = MagicMock()
    mock_conn.cursor.side_effect = RuntimeError("boom")

    with patch("orchestration.sql_runner.psycopg2.connect", return_value=mock_conn):
        with pytest.raises(RuntimeError):
            executer_fichier_sql(str(sql_file), pg_config=FAKE_PG_CONFIG)

    mock_conn.close.assert_called_once()
