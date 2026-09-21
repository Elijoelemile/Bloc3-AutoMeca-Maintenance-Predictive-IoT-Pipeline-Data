"""Client Object Storage partage (data lake centralise, voir diagramme).

OVHcloud Object Storage est compatible S3 : on utilise boto3 comme
pour n'importe quel stockage S3. Ce module est le point d'entree
unique utilise a la fois par le flux batch (extraction/) et le flux
temps reel (ext_load_streaming/) — coherent avec le data lake centralise et
partage du diagramme.
"""
import boto3
from botocore.client import BaseClient

from common.config import ObjectStorageConfig, load_object_storage_config
from common.logging_config import get_logger

logger = get_logger(__name__)


def get_client(config: ObjectStorageConfig | None = None) -> BaseClient:
    cfg = config or load_object_storage_config()
    return boto3.client(
        "s3",
        endpoint_url=cfg.endpoint,
        aws_access_key_id=cfg.access_key,
        aws_secret_access_key=cfg.secret_key,
    )


def upload_file(local_path: str, object_key: str, config: ObjectStorageConfig | None = None) -> None:
    """Depose un fichier brut dans le data lake, sans transformation."""
    cfg = config or load_object_storage_config()
    client = get_client(cfg)
    logger.info("Depot data lake : %s -> s3://%s/%s", local_path, cfg.bucket, object_key)
    client.upload_file(local_path, cfg.bucket, object_key)
    logger.info("Depot reussi : %s", object_key)


def download_file(object_key: str, local_path: str, config: ObjectStorageConfig | None = None) -> None:
    cfg = config or load_object_storage_config()
    client = get_client(cfg)
    logger.info("Lecture data lake : s3://%s/%s -> %s", cfg.bucket, object_key, local_path)
    client.download_file(cfg.bucket, object_key, local_path)
