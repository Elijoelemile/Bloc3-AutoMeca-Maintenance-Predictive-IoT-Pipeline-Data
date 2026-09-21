"""Tests unitaires — extraction/extract_kaggle.py

Aucune vraie connexion Kaggle ni Object Storage : tout est mocke pour
tester uniquement la logique (quels fichiers, quelles cles, quelle
gestion d'erreur).
"""
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from extraction.extract_kaggle import (
    BATCH_FILES,
    ExtractionError,
    extract_and_load,
)


@patch("extraction.extract_kaggle.upload_file")
@patch("extraction.extract_kaggle._kaggle_client")
def test_extract_and_load_uploads_all_batch_files(mock_client_factory, mock_upload, tmp_path, monkeypatch):
    mock_api = MagicMock()

    def fake_download(dataset, file_name, path, force):
        (Path(path) / file_name).write_text("col_a,col_b\n1,2\n")

    mock_api.dataset_download_file.side_effect = fake_download
    mock_client_factory.return_value = mock_api

    run_date = date(2026, 1, 15)
    deposited = extract_and_load(run_date=run_date)

    assert len(deposited) == len(BATCH_FILES)
    assert mock_upload.call_count == len(BATCH_FILES)
    for filename in BATCH_FILES:
        expected_key = f"raw/gmao_erp/2026-01-15/{filename}"
        assert expected_key in deposited


@patch("extraction.extract_kaggle.upload_file")
@patch("extraction.extract_kaggle._kaggle_client")
def test_extract_and_load_raises_on_missing_file(mock_client_factory, mock_upload):
    mock_api = MagicMock()
    mock_api.dataset_download_file.side_effect = lambda **kwargs: None  # ne cree jamais le fichier
    mock_client_factory.return_value = mock_api

    with pytest.raises(ExtractionError):
        extract_and_load(run_date=date(2026, 1, 15))

    mock_upload.assert_not_called()


def test_batch_files_excludes_telemetry():
    """La telemetrie passe par le flux temps reel, pas par cette extraction batch."""
    assert "PdM_telemetry.csv" not in BATCH_FILES
    assert len(BATCH_FILES) == 4
