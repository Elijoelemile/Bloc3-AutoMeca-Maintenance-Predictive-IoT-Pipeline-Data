"""Test fonctionnel — transform/transform_datamart.sql contre un vrai PostgreSQL.

Rejoue ce qu'on avait verifie a la main pendant la construction du
Bloc 3 : fusion correcte staging -> datamart, idempotence reelle sur
une deuxieme execution (verifiee, pas supposee).
"""
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
TRANSFORM_DATAMART_SQL = (REPO_ROOT / "transform" / "transform_datamart.sql").read_text(encoding="utf-8")


def _apply_transform(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute(TRANSFORM_DATAMART_SQL)
    pg_conn.commit()


def test_transform_datamart_fuses_sources_and_is_idempotent(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO staging.machines (machine_id, model, age) VALUES (201, 'model4', 12), (202, 'model2', 3)")
        cur.execute(
            "INSERT INTO staging.erreurs (datetime_evt, machine_id, error_id) VALUES (%s, 201, 'error3')",
            (datetime(2030, 1, 5, 7, 0),),
        )
        cur.execute(
            "INSERT INTO staging.pannes (datetime_evt, machine_id, failure_comp) VALUES (%s, 202, 'comp1')",
            (datetime(2030, 1, 5, 6, 0),),
        )
        cur.execute(
            "INSERT INTO staging.maintenances (datetime_evt, machine_id, comp) VALUES (%s, 201, 'comp2')",
            (datetime(2030, 1, 6, 6, 0),),
        )
    pg_conn.commit()

    _apply_transform(pg_conn)

    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT dt.code_type, dc.code_brut, dm.machine_id_nat
            FROM datamart.fait_evenement fe
            JOIN datamart.dim_machine dm ON dm.id_machine = fe.id_machine
            JOIN datamart.dim_type_evenement dt ON dt.id_type_evenement = fe.id_type_evenement
            JOIN datamart.dim_code_evenement dc ON dc.id_code = fe.id_code
            WHERE dm.machine_id_nat IN (201, 202)
            ORDER BY dm.machine_id_nat, dt.code_type
            """
        )
        rows = cur.fetchall()

    assert rows == [
        ("ERREUR", "error3", 201),
        ("MAINTENANCE", "comp2", 201),
        ("PANNE", "comp1", 202),
    ]

    # Rejoue : aucune ligne dupliquee (idempotence reelle, pas supposee)
    _apply_transform(pg_conn)
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM datamart.fait_evenement fe "
            "JOIN datamart.dim_machine dm ON dm.id_machine = fe.id_machine "
            "WHERE dm.machine_id_nat IN (201, 202)"
        )
        assert cur.fetchone()[0] == 3
