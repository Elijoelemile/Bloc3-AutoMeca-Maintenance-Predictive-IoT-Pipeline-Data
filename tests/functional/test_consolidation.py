"""Test fonctionnel — transform/consolidation.py contre de vrais PostgreSQL + ClickHouse.

Complete les tests unitaires (simules) : verifie le comportement reel de
la jointure cross-moteur (aucun des deux moteurs ne fait la jointure
lui-meme, voir la discussion "TRANSFORM (Python)" du diagramme) et
l'idempotence reelle de l'upsert sur une deuxieme execution.
"""
from datetime import date, datetime
from pathlib import Path

from transform.consolidation import (
    build_consolidation_rows,
    fetch_evenements_jour,
    fetch_machine_id_map,
    fetch_telemetrie_agregats,
    upsert_consolidation,
)

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
TRANSFORM_DATAMART_SQL = (REPO_ROOT / "transform" / "transform_datamart.sql").read_text(encoding="utf-8")


def test_consolidation_is_idempotent_and_correct(pg_conn, ch_client):
    run_date = date(2030, 3, 10)

    # Prerequis : peupler dim_machine + fait_evenement (via transform_datamart.sql)
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO staging.machines (machine_id, model, age) VALUES (501, 'model2', 8)")
        cur.execute(
            "INSERT INTO staging.pannes (datetime_evt, machine_id, failure_comp) VALUES (%s, 501, 'comp3')",
            (datetime(2030, 3, 10, 8, 0),),
        )
        cur.execute(TRANSFORM_DATAMART_SQL)
    pg_conn.commit()

    # Telemetrie ClickHouse pour cette machine, ce jour
    ch_client.insert(
        "automeca.telemetrie",
        [
            (501, datetime(2030, 3, 10, 8, 0, 10), 170, 400, 100, 30),
            (501, datetime(2030, 3, 10, 8, 30, 10), 170, 400, 110, 50),
        ],
        column_names=["machine_id", "datetime_mes", "volt", "rotate", "pressure", "vibration"],
    )

    telemetrie = fetch_telemetrie_agregats(ch_client, run_date)
    evenements = fetch_evenements_jour(pg_conn, run_date)
    machine_id_map = fetch_machine_id_map(pg_conn)
    rows = build_consolidation_rows(telemetrie, evenements, machine_id_map, run_date)

    upsert_consolidation(pg_conn, rows)
    upsert_consolidation(pg_conn, rows)  # rejoue : DO UPDATE, pas de doublon

    with pg_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM datamart.fait_telemetrie_jour")
        total = cur.fetchone()[0]
        assert total == len(machine_id_map)  # grille dense : une ligne par machine connue

        cur.execute(
            """
            SELECT f.rms_vibration_moyen, f.rms_vibration_max, f.nb_pannes
            FROM datamart.fait_telemetrie_jour f
            JOIN datamart.dim_machine dm ON dm.id_machine = f.id_machine
            WHERE dm.machine_id_nat = 501
            """
        )
        rms_moyen, rms_max, nb_pannes = cur.fetchone()

    assert round(rms_moyen, 3) == round((30 + 50) / 2, 3)
    assert rms_max == 50
    assert nb_pannes == 1
