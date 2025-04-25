import os
import asyncio
import aioboto3
import logging

logger = logging.getLogger(__name__)


async def upload_file_to_s3(
    file_path: str, bucket: str, key: str, region_name: str = "us-east-1"
):
    """
    Upload the given file to S3 using aioboto3.
    :param file_path: path to the local file to upload.
    :param bucket: S3 bucket name.
    :param key: S3 object key.
    :param region_name: AWS region, default 'us-east-1'.
    """
    session = aioboto3.Session()
    async with session.client("s3", region_name=region_name) as s3:
        await s3.upload_file(file_path, bucket, key)
    logger.info(f"Uploaded {file_path} to s3://{bucket}/{key}")
