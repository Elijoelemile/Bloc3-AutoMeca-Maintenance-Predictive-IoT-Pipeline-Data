"""Chargement brut — data lake -> staging PostgreSQL (Load, page 1).

Lit les fichiers deposes dans Object Storage par extract_kaggle.py et
les copie tels quels dans les tables staging.* definies au Bloc 2
(01_staging.sql). Aucune transformation ici : les valeurs restent
identiques a la source, seul le contenant change (fichier -> table),
voir la distinction Load/Transform etablie pour ce projet.
"""
import csv
import io
from datetime import date

import psycopg2
import psycopg2.extras

from common.config import load_postgres_config
from common.logging_config import get_logger
from common.object_storage import get_client, load_object_storage_config

logger = get_logger(__name__)

# Association fichier source -> (table staging, colonnes dans l'ordre du CSV)
STAGING_TARGETS = {
    "PdM_machines.csv": ("staging.machines", ["machine_id", "model", "age"]),
    "PdM_errors.csv": ("staging.erreurs", ["datetime_evt", "machine_id", "error_id"]),
    "PdM_failures.csv": ("staging.pannes", ["datetime_evt", "machine_id", "failure_comp"]),
    "PdM_maint.csv": ("staging.maintenances", ["datetime_evt", "machine_id", "comp"]),
}


def _read_object_as_rows(object_key: str) -> list[dict]:
    os_config = load_object_storage_config()
    client = get_client(os_config)
    logger.info("Lecture data lake pour chargement staging : %s", object_key)
    body = client.get_object(Bucket=os_config.bucket, Key=object_key)["Body"].read()
    reader = csv.DictReader(io.StringIO(body.decode("utf-8")))
    return list(reader)


def load_file_to_staging(object_key: str, filename: str, conn) -> int:
    """Charge un fichier brut du data lake vers sa table staging. Retourne le nb de lignes."""
    if filename not in STAGING_TARGETS:
        raise ValueError(f"Pas de table staging associee a {filename}")
    table, columns = STAGING_TARGETS[filename]
    rows = _read_object_as_rows(object_key)

    values = [tuple(row[col] for col in columns) for row in rows]
    if not values:
        logger.warning("Aucune ligne a charger pour %s", filename)
        return 0

    columns_sql = ", ".join(columns)
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            f"INSERT INTO {table} ({columns_sql}) VALUES %s ON CONFLICT DO NOTHING",
            values,
        )
    conn.commit()
    logger.info("Charge dans %s : %d lignes (doublons ignores)", table, len(values))
    return len(values)


def load_batch_to_staging(run_date: date | None = None) -> dict[str, int]:
    run_date = run_date or date.today()
    pg_config = load_postgres_config()
    conn = psycopg2.connect(
        host=pg_config.host, port=pg_config.port, dbname=pg_config.database,
        user=pg_config.user, password=pg_config.password,
    )
    counts: dict[str, int] = {}
    try:
        for filename in STAGING_TARGETS:
            object_key = f"raw/gmao_erp/{run_date.isoformat()}/{filename}"
            counts[filename] = load_file_to_staging(object_key, filename, conn)
    finally:
        conn.close()
    return counts


if __name__ == "__main__":
    load_batch_to_staging()
