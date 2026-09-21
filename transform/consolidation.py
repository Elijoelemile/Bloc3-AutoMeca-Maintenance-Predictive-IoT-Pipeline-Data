"""Consolidation quotidienne — telemetrie ClickHouse + datamart PostgreSQL
(Transform, page 1 du diagramme).

Jointure croisee-moteur : ni ClickHouse ni PostgreSQL ne peut interroger
nativement l'autre sans connecteur externe fragile (voir discussion
Bloc 3 — c'est pourquoi ce composant est en Python, pas en SQL, marque
"TRANSFORM (Python)" sur le diagramme). Le script lit chaque source
separement, fusionne en memoire (volumes negligeables : une centaine de
machines, quelques dizaines de lignes/jour), puis ecrit le resultat
dans datamart.fait_telemetrie_jour (transform/01_add_fait_telemetrie_
jour.sql).

Grille dense : une ligne est ecrite pour CHAQUE machine connue de
dim_machine, meme sans telemetrie ni evenement ce jour-la (valeurs
NULL/0) — plus utile pour l'entrainement du modele (Bloc 4) qu'une
union creuse qui masquerait silencieusement les jours sans donnee.

Idempotent : ON CONFLICT (id_date, id_machine) DO UPDATE, rejouable
sans creer de doublon (meme principe que transform_datamart.sql).
"""
import clickhouse_connect
import psycopg2
import psycopg2.extras
from datetime import date

from common.config import load_clickhouse_config, load_postgres_config
from common.logging_config import get_logger

logger = get_logger(__name__)


def fetch_telemetrie_agregats(clickhouse_client, run_date: date) -> dict[int, dict]:
    """Agrege les fenetres d'1 minute (vue materialisee) en un resume quotidien par machine."""
    result = clickhouse_client.query(
        """
        SELECT
            machine_id,
            avg(rms_vibration) AS rms_vibration_moyen,
            max(rms_vibration) AS rms_vibration_max,
            avg(tendance_pression) AS tendance_pression_moyenne
        FROM automeca.v_telemetrie_tendance
        WHERE toDate(fenetre) = %(run_date)s
        GROUP BY machine_id
        """,
        parameters={"run_date": run_date},
    )
    return {
        row[0]: {
            "rms_vibration_moyen": row[1],
            "rms_vibration_max": row[2],
            "tendance_pression_moyenne": row[3],
        }
        for row in result.result_rows
    }


def fetch_evenements_jour(conn, run_date: date) -> dict[int, dict]:
    """Compte les evenements (erreurs/pannes/maintenances) du jour, par machine (cle naturelle)."""
    id_date = int(run_date.strftime("%Y%m%d"))
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT dm.machine_id_nat,
                   count(*) FILTER (WHERE dt.code_type = 'ERREUR') AS nb_erreurs,
                   count(*) FILTER (WHERE dt.code_type = 'PANNE') AS nb_pannes,
                   count(*) FILTER (WHERE dt.code_type = 'MAINTENANCE') AS nb_maintenances
            FROM datamart.fait_evenement fe
            JOIN datamart.dim_machine dm ON dm.id_machine = fe.id_machine
            JOIN datamart.dim_type_evenement dt ON dt.id_type_evenement = fe.id_type_evenement
            WHERE fe.id_date = %(id_date)s
            GROUP BY dm.machine_id_nat
            """,
            {"id_date": id_date},
        )
        rows = cur.fetchall()
    return {
        row[0]: {"nb_erreurs": row[1], "nb_pannes": row[2], "nb_maintenances": row[3]}
        for row in rows
    }


def fetch_machine_id_map(conn) -> dict[int, int]:
    """machine_id_nat (cle naturelle, cote ClickHouse/staging) -> id_machine (cle de substitution datamart)."""
    with conn.cursor() as cur:
        cur.execute("SELECT machine_id_nat, id_machine FROM datamart.dim_machine")
        return dict(cur.fetchall())


def build_consolidation_rows(
    telemetrie: dict[int, dict],
    evenements: dict[int, dict],
    machine_id_map: dict[int, int],
    run_date: date,
) -> list[tuple]:
    id_date = int(run_date.strftime("%Y%m%d"))
    rows = []
    for machine_id_nat, id_machine in machine_id_map.items():
        t = telemetrie.get(machine_id_nat, {})
        e = evenements.get(machine_id_nat, {"nb_erreurs": 0, "nb_pannes": 0, "nb_maintenances": 0})
        rows.append((
            id_date,
            id_machine,
            t.get("rms_vibration_moyen"),
            t.get("rms_vibration_max"),
            t.get("tendance_pression_moyenne"),
            e["nb_erreurs"],
            e["nb_pannes"],
            e["nb_maintenances"],
        ))
    return rows


def upsert_consolidation(conn, rows: list[tuple]) -> None:
    if not rows:
        logger.warning("Aucune ligne de consolidation a ecrire")
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO datamart.fait_telemetrie_jour
                (id_date, id_machine, rms_vibration_moyen, rms_vibration_max,
                 tendance_pression_moyenne, nb_erreurs, nb_pannes, nb_maintenances)
            VALUES %s
            ON CONFLICT (id_date, id_machine) DO UPDATE SET
                rms_vibration_moyen = EXCLUDED.rms_vibration_moyen,
                rms_vibration_max = EXCLUDED.rms_vibration_max,
                tendance_pression_moyenne = EXCLUDED.tendance_pression_moyenne,
                nb_erreurs = EXCLUDED.nb_erreurs,
                nb_pannes = EXCLUDED.nb_pannes,
                nb_maintenances = EXCLUDED.nb_maintenances
            """,
            rows,
        )
    conn.commit()


def get_clickhouse_client(config=None):
    cfg = config or load_clickhouse_config()
    return clickhouse_connect.get_client(
        host=cfg.host, port=cfg.port, database=cfg.database,
        username=cfg.user, password=cfg.password,
    )


def consolidate(run_date: date | None = None) -> int:
    run_date = run_date or date.today()
    pg_config = load_postgres_config()

    clickhouse_client = get_clickhouse_client()
    conn = psycopg2.connect(
        host=pg_config.host, port=pg_config.port, dbname=pg_config.database,
        user=pg_config.user, password=pg_config.password,
    )
    try:
        telemetrie = fetch_telemetrie_agregats(clickhouse_client, run_date)
        evenements = fetch_evenements_jour(conn, run_date)
        machine_id_map = fetch_machine_id_map(conn)
        rows = build_consolidation_rows(telemetrie, evenements, machine_id_map, run_date)
        upsert_consolidation(conn, rows)
        logger.info("Consolidation %s : %d machines", run_date.isoformat(), len(rows))
        return len(rows)
    finally:
        conn.close()


if __name__ == "__main__":
    consolidate()
