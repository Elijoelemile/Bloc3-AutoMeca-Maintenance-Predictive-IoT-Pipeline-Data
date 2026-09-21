"""Tests unitaires — orchestration/dag_pipeline_bloc3.py

Ne teste que la logique non-Airflow (le callback d'alerte). Les DAG
eux-memes (definis via les decorateurs @dag/@task) ne sont pas
retestes ici : ce sont de simples enchainements de fonctions deja
testees individuellement (extraction, chargement_staging, etc.).
"""
from unittest.mock import MagicMock, patch

from orchestration.dag_pipeline_bloc3 import alerter_echec_operationnel


def test_alerter_echec_operationnel_logs_task_and_dag_id():
    context = {
        "task_instance": MagicMock(task_id="transformation_datamart"),
        "dag": MagicMock(dag_id="pipeline_batch_bloc3"),
    }

    with patch("orchestration.dag_pipeline_bloc3.logger") as mock_logger:
        alerter_echec_operationnel(context)

    mock_logger.error.assert_called_once()
    args, _ = mock_logger.error.call_args
    assert "transformation_datamart" in args
    assert "pipeline_batch_bloc3" in args
