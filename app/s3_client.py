# app/s3_client.py
import io
import boto3
from .config import get_settings

settings = get_settings()

_session = boto3.session.Session()

s3 = _session.client(
    "s3",
    endpoint_url=f"http{'s' if settings.s3_use_ssl else ''}://{settings.s3_endpoint}",
    aws_access_key_id=settings.s3_access_key,
    aws_secret_access_key=settings.s3_secret_key,
)


def download_object_to_bytes(bucket: str, key: str) -> bytes:
    """
    Download the S3 object and return raw bytes.
    """
    resp = s3.get_object(Bucket=bucket, Key=key)
    return resp["Body"].read()
