"""
iBot v2 GCS Utilities

Handles Google Cloud Storage operations like moving files
between folders (archive, failed).
"""

from typing import List, Optional
from google.cloud import storage

from utils.logger import get_logger

logger = get_logger(__name__)

# Singleton storage client
_storage_client: Optional[storage.Client] = None


def get_storage_client() -> storage.Client:
    """Get or create GCS client."""
    global _storage_client
    if _storage_client is None:
        _storage_client = storage.Client()
    return _storage_client


def move_blob(
    bucket_name: str,
    source_path: str,
    destination_path: str,
) -> bool:
    """
    Move a blob from source to destination within the same bucket.

    This is a copy + delete operation since GCS doesn't have native move.

    Args:
        bucket_name: GCS bucket name
        source_path: Current blob path
        destination_path: New blob path

    Returns:
        True if successful, False otherwise
    """
    try:
        client = get_storage_client()
        bucket = client.bucket(bucket_name)

        source_blob = bucket.blob(source_path)

        # Check if source exists
        if not source_blob.exists():
            logger.warning(
                "Source blob does not exist",
                bucket=bucket_name,
                path=source_path,
            )
            return False

        # Copy to destination
        destination_blob = bucket.blob(destination_path)

        # Use rewrite for large files (handles files > 5GB)
        rewrite_token = None
        while True:
            rewrite_token, bytes_rewritten, total_bytes = destination_blob.rewrite(
                source_blob, token=rewrite_token
            )
            if rewrite_token is None:
                break

        logger.info(
            "Copied blob",
            source=source_path,
            destination=destination_path,
        )

        # Delete source
        source_blob.delete()

        logger.info(
            "Moved blob successfully",
            source=source_path,
            destination=destination_path,
        )

        return True

    except Exception as e:
        logger.error(
            "Failed to move blob",
            source=source_path,
            destination=destination_path,
            error=str(e),
            exc_info=True,
        )
        return False


def move_to_archive(bucket_name: str, blob_path: str) -> bool:
    """
    Move a processed file to the archive folder.

    Example:
        BA Produk LAZ/GS data.xlsx -> archive/BA Produk LAZ/GS data.xlsx

    Args:
        bucket_name: GCS bucket name
        blob_path: Current blob path

    Returns:
        True if successful
    """
    archive_path = f"archive/{blob_path}"

    logger.info(
        "Moving to archive",
        source=blob_path,
        destination=archive_path,
    )

    return move_blob(bucket_name, blob_path, archive_path)


def move_to_failed(bucket_name: str, blob_path: str) -> bool:
    """
    Move a failed file to the failed folder.

    Example:
        BA Produk LAZ/GS data.xlsx -> failed/BA Produk LAZ/GS data.xlsx

    Args:
        bucket_name: GCS bucket name
        blob_path: Current blob path

    Returns:
        True if successful
    """
    failed_path = f"failed/{blob_path}"

    logger.info(
        "Moving to failed",
        source=blob_path,
        destination=failed_path,
    )

    return move_blob(bucket_name, blob_path, failed_path)


def list_blobs_in_folder(
    bucket_name: str,
    folder_path: str,
    extensions: tuple = (".xlsx", ".xls", ".csv"),
) -> List[str]:
    """
    List all blobs in a folder with matching extensions.

    Args:
        bucket_name: GCS bucket name
        folder_path: Folder path (e.g., "BA Produk LAZ")
        extensions: File extensions to include

    Returns:
        List of blob names
    """
    try:
        client = get_storage_client()
        bucket = client.bucket(bucket_name)

        # Ensure folder path ends with /
        if folder_path and not folder_path.endswith("/"):
            folder_path = f"{folder_path}/"

        blobs = bucket.list_blobs(prefix=folder_path)

        matching_blobs = []
        for blob in blobs:
            # Skip folder markers
            if blob.name.endswith("/"):
                continue
            # Check extension
            if blob.name.lower().endswith(extensions):
                matching_blobs.append(blob.name)

        logger.info(
            "Listed blobs in folder",
            folder=folder_path,
            count=len(matching_blobs),
        )

        return matching_blobs

    except Exception as e:
        logger.error(
            "Failed to list blobs",
            folder=folder_path,
            error=str(e),
        )
        return []


def download_blob_as_bytes(bucket_name: str, blob_path: str) -> Optional[bytes]:
    """
    Download a blob's content as bytes.

    Args:
        bucket_name: GCS bucket name
        blob_path: Path to the blob

    Returns:
        File content as bytes, or None if failed
    """
    try:
        client = get_storage_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        content = blob.download_as_bytes()

        logger.info(
            "Downloaded blob",
            path=blob_path,
            size_bytes=len(content),
        )

        return content

    except Exception as e:
        logger.error(
            "Failed to download blob",
            path=blob_path,
            error=str(e),
        )
        return None
