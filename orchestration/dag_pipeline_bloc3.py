"""Orchestration Airflow — Bloc 3 (page 1 du diagramme, bande "Orchestration").

Deux DAG distincts :
- **pipeline_batch_bloc3** (quotidien) : extraction -> staging ->
  datamart -> consolidation, dans cet ordre strict (chaque etape
  depend de la precedente, toutes idempotentes — verifie
  empiriquement, voir transform/ et extraction/).
- **surveillance_capteurs_bloc3** (toutes les minutes) : controle
  "capteur silencieux" (quality/validation_rules.py) — periodique par
  nature, pas un filtre par message.

Hors perimetre, volontairement : les deux processus temps reel
(ext_load_streaming/mqtt_to_kafka.py, kafka_to_clickhouse.py) tournent
en continu, pas sur un planning — ce ne sont pas des taches Airflow
mais des processus persistants, supervises par l'environnement
d'execution (redemarrage automatique en cas de crash). Voir le README
pour la discussion complete de cette limite de perimetre.

Resilience : chaque tache beneficie d'une reprise automatique sur
erreur (retries ci-dessous), sans risque de doublon grace a
l'idempotence des operations sous-jacentes. Un echec declenche une
alerte operationnelle journalisee (on_failure_callback) — distincte de
l'alerte precoce metier (RMS/tendance, Grafana). Le canal de
notification reel (email/Slack/PagerDuty) sera configure au
provisionnement OVHcloud (voir README) : ce fichier ne fabrique pas de
canal factice.
"""
from datetime import datetime, timedelta

import psycopg2
from airflow.decorators import dag, task

from common.config import load_postgres_config
from common.logging_config import get_logger
from ext_load_streaming.kafka_to_clickhouse import get_clickhouse_client
from extraction.extract_kaggle import extract_and_load
from extraction.load_staging import load_batch_to_staging
from orchestration.sql_runner import executer_fichier_sql
from quality.validation_rules import verifier_capteurs_silencieux
from transform.consolidation import consolidate

logger = get_logger(__name__)

TRANSFORM_DATAMART_SQL = "transform/transform_datamart.sql"

def alerter_echec_operationnel(context) -> None:
    """Alerte operationnelle (echec de tache pipeline) — distincte de
    l'alerte precoce metier declenchee par la vue materialisee ClickHouse."""
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    logger.error("ALERTE OPERATIONNELLE : echec de la tache %s (DAG %s)", task_id, dag_id)


# on_failure_callback ici (pas au niveau @dag) : declenche l'alerte a
# l'echec de CHAQUE tache individuelle, pas seulement si le DAG entier
# echoue — cf. "chaque tache... declenche une alerte" (README).
DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alerter_echec_operationnel,
}


@dag(
    dag_id="pipeline_batch_bloc3",
    description="Flux batch quotidien — extraction -> staging -> datamart -> consolidation",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
)
def pipeline_batch_bloc3():

    @task
    def extraction():
        extract_and_load()

    @task
    def chargement_staging():
        load_batch_to_staging()

    @task
    def transformation_datamart():
        executer_fichier_sql(TRANSFORM_DATAMART_SQL)

    @task
    def consolidation_quotidienne():
        consolidate()

    extraction() >> chargement_staging() >> transformation_datamart() >> consolidation_quotidienne()


pipeline_batch_bloc3()


@dag(
    dag_id="surveillance_capteurs_bloc3",
    description='Controle periodique "capteur silencieux" (quality/validation_rules.py)',
    schedule=timedelta(minutes=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
)
def surveillance_capteurs_bloc3():

    @task
    def verifier_silence():
        pg_config = load_postgres_config()
        conn = psycopg2.connect(
            host=pg_config.host, port=pg_config.port, dbname=pg_config.database,
            user=pg_config.user, password=pg_config.password,
        )
        try:
            clickhouse_client = get_clickhouse_client()
            verifier_capteurs_silencieux(clickhouse_client, conn)
        finally:
            conn.close()

    verifier_silence()


surveillance_capteurs_bloc3()
