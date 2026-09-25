"""Test fonctionnel — validite reelle du DAG Airflow (DagBag reel, rien
de simule). Aurait detecte automatiquement le bug on_failure_callback
trouve pendant la construction (place au niveau @dag au lieu de
default_args — ne se declenchait qu'a l'echec du DAG entier, pas de
chaque tache). Ne necessite pas Docker : Airflow lui-meme est le
"moteur reel" ici.
"""
from airflow.models import DagBag


def _load_dag_bag() -> DagBag:
    return DagBag(dag_folder="orchestration", include_examples=False)


def test_dag_bag_parses_without_import_errors():
    bag = _load_dag_bag()
    assert bag.import_errors == {}


def test_pipeline_batch_bloc3_task_order():
    bag = _load_dag_bag()
    dag = bag.dags["pipeline_batch_bloc3"]

    task_ids = [t.task_id for t in dag.tasks]
    assert task_ids == [
        "extraction", "chargement_staging", "transformation_datamart", "consolidation_quotidienne",
    ]

    by_id = {t.task_id: t for t in dag.tasks}
    assert [d.task_id for d in by_id["extraction"].downstream_list] == ["chargement_staging"]
    assert [d.task_id for d in by_id["chargement_staging"].downstream_list] == ["transformation_datamart"]
    assert [d.task_id for d in by_id["transformation_datamart"].downstream_list] == ["consolidation_quotidienne"]
    assert by_id["consolidation_quotidienne"].downstream_list == []


def test_default_args_apply_retry_and_alert_to_every_task():
    bag = _load_dag_bag()
    for dag_id in ["pipeline_batch_bloc3", "surveillance_capteurs_bloc3"]:
        dag = bag.dags[dag_id]
        assert dag.default_args.get("retries") == 3
        assert dag.default_args.get("on_failure_callback") is not None
