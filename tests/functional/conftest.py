"""Fixtures partagees pour tests/functional/ — infrastructure reelle
(PostgreSQL, ClickHouse) via testcontainers, aucun mock.

Conteneurs a portee "module" : le demarrage Docker coute plusieurs
secondes, partage entre les tests d'un meme fichier. Chaque test
utilise des donnees distinctes (machine_id/dates propres) pour rester
isole sans avoir a truncate entre chaque test.
"""
import re
import time
from pathlib import Path

import clickhouse_connect
import psycopg2
import pytest
from testcontainers.community.clickhouse import ClickHouseContainer
from testcontainers.community.postgres import PostgresContainer

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
FIXTURES = Path(__file__).resolve().parent / "fixtures"

POSTGRES_DDL = [
    FIXTURES / "bloc2_01_staging.sql",
    FIXTURES / "bloc2_02_datamart.sql",
    REPO_ROOT / "transform" / "00_add_missing_constraints.sql",
    REPO_ROOT / "transform" / "01_add_fait_telemetrie_jour.sql",
    REPO_ROOT / "privacy" / "00_widen_matricule_column.sql",
]

CLICKHOUSE_DDL = [
    FIXTURES / "bloc2_03_telemetrie_clickhouse.sql",
    REPO_ROOT / "quality" / "00_add_quarantine_table.sql",
    REPO_ROOT / "transform" / "materialized_view_telemetrie.sql",
    REPO_ROOT / "dashboard" / "00_add_ingestion_timestamp.sql",
]


def _split_sql_statements(sql_text: str) -> list[str]:
    """ClickHouse (contrairement a psycopg2) n'execute qu'une instruction
    par appel — decoupage naif sur ';', adequat pour nos fichiers DDL
    (pas de point-virgule dans une chaine litterale)."""
    sans_commentaires = "\n".join(
        line for line in sql_text.splitlines() if not line.strip().startswith("--")
    )
    return [s.strip() for s in re.split(r";\s*\n|;\s*$", sans_commentaires) if s.strip()]


@pytest.fixture(scope="module")
def postgres_container():
    with PostgresContainer("postgres:16") as pg:
        conn = psycopg2.connect(
            host=pg.get_container_host_ip(), port=pg.get_exposed_port(5432),
            dbname=pg.dbname, user=pg.username, password=pg.password,
        )
        with conn.cursor() as cur:
            for path in POSTGRES_DDL:
                cur.execute(path.read_text(encoding="utf-8"))
        conn.commit()
        conn.close()
        yield pg


@pytest.fixture
def pg_conn(postgres_container):
    conn = psycopg2.connect(
        host=postgres_container.get_container_host_ip(),
        port=postgres_container.get_exposed_port(5432),
        dbname=postgres_container.dbname,
        user=postgres_container.username,
        password=postgres_container.password,
    )
    yield conn
    conn.close()


def _start_clickhouse_with_retry(attempts: int = 3) -> ClickHouseContainer:
    """Le mapping de port n'est parfois pas encore visible juste apres le
    demarrage du conteneur (course, observee sur cet environnement Docker
    Desktop/Windows) — quelques tentatives suffisent, ce n'est pas un bug
    de notre code."""
    last_exc: Exception | None = None
    for _ in range(attempts):
        container = ClickHouseContainer("clickhouse/clickhouse-server:24")
        try:
            container.start()
            return container
        except Exception as exc:  # noqa: BLE001 — retry volontairement large, erreur re-levee au final
            last_exc = exc
            try:
                container.stop()
            except Exception:
                pass
            time.sleep(2)
    raise last_exc


@pytest.fixture(scope="module")
def clickhouse_container():
    ch = _start_clickhouse_with_retry()
    try:
        client = clickhouse_connect.get_client(
            host=ch.get_container_host_ip(),
            port=int(ch.get_exposed_port(8123)),
            username=ch.username,
            password=ch.password,
        )
        for path in CLICKHOUSE_DDL:
            for statement in _split_sql_statements(path.read_text(encoding="utf-8")):
                client.command(statement)
        client.close()
        yield ch
    finally:
        ch.stop()


@pytest.fixture
def ch_client(clickhouse_container):
    client = clickhouse_connect.get_client(
        host=clickhouse_container.get_container_host_ip(),
        port=int(clickhouse_container.get_exposed_port(8123)),
        username=clickhouse_container.username,
        password=clickhouse_container.password,
    )
    yield client
    client.close()
