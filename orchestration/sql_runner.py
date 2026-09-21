"""Execution d'un fichier .sql contre PostgreSQL — utilise par le DAG
pour la tache "transformation_datamart" (transform/transform_datamart.sql).

Separe du fichier DAG pour rester testable sans environnement Airflow
(voir tests/unit/test_sql_runner.py).
"""
import psycopg2

from common.config import PostgresConfig, load_postgres_config
from common.logging_config import get_logger

logger = get_logger(__name__)


def executer_fichier_sql(chemin_sql: str, pg_config: PostgresConfig | None = None) -> None:
    """Lit un fichier .sql et l'execute en une fois contre PostgreSQL (commit a la fin)."""
    cfg = pg_config or load_postgres_config()
    with open(chemin_sql, encoding="utf-8") as f:
        sql = f.read()

    conn = psycopg2.connect(
        host=cfg.host, port=cfg.port, dbname=cfg.database,
        user=cfg.user, password=cfg.password,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        logger.info("Fichier SQL execute : %s", chemin_sql)
    finally:
        conn.close()
