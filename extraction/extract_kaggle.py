"""Extraction batch quotidienne — GMAO/ERP (Extract, voir diagramme page 1).

Recupere les 4 fichiers "evenements/referentiel" du dataset Kaggle
(machines, erreurs, pannes, maintenances) et les depose bruts dans le
data lake centralise (Object Storage). N'inclut PAS la telemetrie :
celle-ci est traitee par le flux temps reel (ext_load_streaming/), pas par
l'extraction batch — voir la separation des deux couloirs au Bloc 3.

Idempotent : peut etre relance sans effet de bord (chaque execution
ecrase le meme chemin date dans le data lake, elle ne l'ajoute pas en
double).
"""
import tempfile
from datetime import date
from pathlib import Path

from kaggle.api.kaggle_api_extended import KaggleApi

from common.logging_config import get_logger
from common.object_storage import upload_file

logger = get_logger(__name__)

KAGGLE_DATASET = "arnabbiswas1/microsoft-azure-predictive-maintenance"

BATCH_FILES = [
    "PdM_machines.csv",
    "PdM_errors.csv",
    "PdM_failures.csv",
    "PdM_maint.csv",
]


class ExtractionError(RuntimeError):
    """Levee quand un fichier attendu n'a pas pu etre recupere."""


def _kaggle_client() -> KaggleApi:
    api = KaggleApi()
    api.authenticate()
    return api


def extract_and_load(run_date: date | None = None) -> list[str]:
    """Extrait les fichiers GMAO/ERP et les depose dans le data lake.

    Retourne la liste des cles objet deposees (pour tracabilite/log).
    """
    run_date = run_date or date.today()
    api = _kaggle_client()
    deposited: list[str] = []

    with tempfile.TemporaryDirectory() as tmp_dir:
        for filename in BATCH_FILES:
            local_path = Path(tmp_dir) / filename
            try:
                logger.info("Extraction Kaggle : %s", filename)
                api.dataset_download_file(
                    KAGGLE_DATASET, file_name=filename, path=tmp_dir, force=True
                )
                if not local_path.exists():
                    # Kaggle peut livrer le fichier compresse (.zip) selon le fichier
                    zip_path = local_path.with_suffix(local_path.suffix + ".zip")
                    if zip_path.exists():
                        import zipfile
                        with zipfile.ZipFile(zip_path) as zf:
                            zf.extractall(tmp_dir)
                if not local_path.exists():
                    raise ExtractionError(f"Fichier absent apres extraction : {filename}")
            except Exception:
                logger.exception("Echec extraction pour %s", filename)
                raise ExtractionError(f"Extraction Kaggle en echec : {filename}") from None

            object_key = f"raw/gmao_erp/{run_date.isoformat()}/{filename}"
            upload_file(str(local_path), object_key)
            deposited.append(object_key)

    logger.info("Extraction batch terminee : %d fichiers deposes", len(deposited))
    return deposited


if __name__ == "__main__":
    extract_and_load()
