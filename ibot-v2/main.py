"""
iBot v2 - Cloud Function Entry Points

Processes Excel/CSV files from GCS bucket and loads to BigQuery.

Triggers:
- GCS finalize event: Auto-process uploaded files
- HTTP: Manual processing, health checks

Folder structure in GCS:
- {CATEGORY}/{BRAND} filename.xlsx  -> Files to process
- archive/{CATEGORY}/...            -> Successfully processed
- failed/{CATEGORY}/...             -> Failed imports
"""

import asyncio
import time
import uuid
from pathlib import Path
from typing import Optional

import functions_framework
from cloudevents.http import CloudEvent
from flask import Request, jsonify

from config import get_category, get_slack_webhook, settings
from parser import parse_file
from bigquery_loader import get_loader
from slack_notifier import get_notifier
from brand_detector import detect_brand_from_data, is_legacy_detection_enabled
from utils.gcs_utils import download_blob_as_bytes, move_to_archive, move_to_failed, upload_blob
from utils.logger import get_logger

logger = get_logger(__name__)


def extract_info_from_path(blob_path: str) -> tuple[Optional[str], Optional[str], str]:
    """
    Extract category, brand code, and filename from GCS blob path.

    Expected format: {CATEGORY}/{BRAND} filename.xlsx
    Example: BA Produk LAZ/GS 2024-01-15_data.xlsx
             -> category="BA Produk LAZ", brand="GS", filename="GS 2024-01-15_data.xlsx"

    Returns:
        Tuple of (category_name, brand_code, filename)
    """
    # Skip special folders
    if blob_path.startswith(("archive/", "failed/", ".")):
        return None, None, ""

    parts = blob_path.split("/")
    if len(parts) < 2:
        return None, None, blob_path

    category_name = parts[0]
    filename = parts[-1]

    # Extract brand code from filename (first word before space)
    brand_code = filename.split(" ")[0] if " " in filename else filename.split("_")[0]
    # Clean up brand code (remove extension if no space)
    if "." in brand_code:
        brand_code = Path(brand_code).stem

    return category_name, brand_code, filename


def extract_brand_from_data(parsed_file) -> Optional[str]:
    """
    Extract brand code from parsed file data.

    Looks for 'akun' column (used in BA Produk files) and returns
    the most common value as the brand code.

    Returns:
        Brand code string, or None if not found
    """
    if not parsed_file.rows:
        return None

    # Look for 'akun' column (case-insensitive)
    akun_key = None
    for header in parsed_file.headers:
        if header.lower() == 'akun':
            akun_key = header
            break

    if not akun_key:
        return None

    # Count occurrences of each value
    value_counts = {}
    for row in parsed_file.rows:
        value = row.get(akun_key, "")
        if value and str(value).strip():
            value = str(value).strip()
            value_counts[value] = value_counts.get(value, 0) + 1

    if not value_counts:
        return None

    # Return the most common value
    brand = max(value_counts.keys(), key=lambda x: value_counts[x])
    logger.info(f"Extracted brand '{brand}' from akun column ({value_counts[brand]} occurrences)")
    return brand


async def process_file(bucket_name: str, blob_path: str) -> dict:
    """
    Process a single file from GCS.

    Args:
        bucket_name: GCS bucket name
        blob_path: Path to file in bucket

    Returns:
        Result dict with success status and details
    """
    start_time = time.time()

    # Extract info from path
    category_name, brand_code, filename = extract_info_from_path(blob_path)

    if not category_name:
        logger.warning(f"Skipping file in special folder: {blob_path}")
        return {"skipped": True, "reason": "special_folder"}

    # Get category config
    category = get_category(category_name)
    if not category:
        logger.error(f"Unknown category: {category_name}", path=blob_path)
        move_to_failed(bucket_name, blob_path)
        return {"success": False, "error": f"Unknown category: {category_name}"}

    logger.info(
        f"Processing file",
        category=category_name,
        brand=brand_code,
        file=filename,
    )

    # Download file content
    file_content = download_blob_as_bytes(bucket_name, blob_path)
    if not file_content:
        logger.error(f"Failed to download file: {blob_path}")
        move_to_failed(bucket_name, blob_path)
        return {"success": False, "error": "Failed to download file"}

    # Parse file
    parsed_file = parse_file(filename, file_content, category)

    if not parsed_file.is_valid:
        error_msg = parsed_file.errors[0] if parsed_file.errors else "No data found"
        logger.error(f"Failed to parse file: {error_msg}", path=blob_path)
        move_to_failed(bucket_name, blob_path)

        # Send failure notification
        notifier = get_notifier(get_slack_webhook(category_name))
        await notifier.notify_failure(filename, brand_code, category_name, error_msg)

        return {"success": False, "error": error_msg}

    # Try to detect brand from file content if filename-based brand looks invalid
    # Valid brand codes: all uppercase like "GS", "BR", "HYDR-M", "ALUN-M", "ASV-M"
    # Invalid: mixed-case words like "Bisnis" (from "Bisnis Analisis...")
    def is_valid_brand_code(code: str) -> bool:
        if not code:
            return False
        # Valid if the code is all uppercase (letters, digits, dashes, underscores allowed)
        return code == code.upper()

    if not is_valid_brand_code(brand_code):
        if is_legacy_detection_enabled():
            # Use v1-style brand detection from product codes
            detected_brand = detect_brand_from_data(parsed_file, category_name)
            if detected_brand:
                logger.info(f"Using detected brand '{detected_brand}' instead of '{brand_code}'")
                brand_code = detected_brand
            else:
                # Fallback: try to get brand from 'akun' column
                extracted_brand = extract_brand_from_data(parsed_file)
                if extracted_brand:
                    logger.info(f"Using extracted brand '{extracted_brand}' instead of '{brand_code}'")
                    brand_code = extracted_brand

    # Generate import ID
    import_id = f"ibotv2_{category.bigquery_table}_{brand_code}_{uuid.uuid4().hex[:8]}"

    # Upload to BigQuery
    if settings.BIGQUERY_ENABLED:
        loader = get_loader()
        success, result = loader.upload(parsed_file, brand_code, import_id)

        if not success:
            error_msg = result.get("error", "BigQuery upload failed")
            logger.error(f"BigQuery upload failed: {error_msg}", path=blob_path)
            move_to_failed(bucket_name, blob_path)

            notifier = get_notifier(get_slack_webhook(category_name))
            await notifier.notify_failure(filename, brand_code, category_name, error_msg)

            return {"success": False, "error": error_msg, "import_id": import_id}

        rows_inserted = result.get("rows_inserted", 0)
    else:
        logger.info("BigQuery disabled, skipping upload")
        rows_inserted = len(parsed_file.rows)
        result = {"rows_inserted": rows_inserted}

    # Move to archive
    move_to_archive(bucket_name, blob_path)

    duration = time.time() - start_time

    # Send success notification
    notifier = get_notifier(get_slack_webhook(category_name))
    await notifier.notify_success(parsed_file, brand_code, import_id, rows_inserted, duration)

    logger.info(
        f"File processed successfully",
        category=category_name,
        brand=brand_code,
        rows=rows_inserted,
        duration=f"{duration:.1f}s",
    )

    return {
        "success": True,
        "import_id": import_id,
        "category": category_name,
        "brand": brand_code,
        "rows_inserted": rows_inserted,
        "duration_seconds": duration,
    }


def run_async(coro):
    """Run async coroutine in sync context."""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


# ============================================================
# Cloud Function Entry Points
# ============================================================

@functions_framework.cloud_event
def process_import(cloud_event: CloudEvent) -> None:
    """
    Process file uploaded to GCS bucket.

    Triggered by: google.cloud.storage.object.v1.finalized
    """
    data = cloud_event.data

    bucket_name = data["bucket"]
    blob_path = data["name"]

    logger.info(
        f"GCS event received",
        bucket=bucket_name,
        path=blob_path,
        event_type=cloud_event["type"],
    )

    # Skip non-data files
    if not blob_path.lower().endswith((".xlsx", ".xls", ".csv")):
        logger.debug(f"Skipping non-data file: {blob_path}")
        return

    # Skip special folders
    if blob_path.startswith(("archive/", "failed/", ".")):
        logger.debug(f"Skipping file in special folder: {blob_path}")
        return

    # Process the file
    result = run_async(process_file(bucket_name, blob_path))

    logger.info(f"Processing complete", result=result)


@functions_framework.http
def http_handler(request: Request):
    """
    HTTP endpoint for manual operations.

    Routes:
        GET  /health  -> Health check
        POST /process -> Process specific file
    """
    path = request.path.rstrip("/")

    if path == "/health" or path == "":
        return jsonify({
            "status": "healthy",
            "service": "ibot-v2",
            "bigquery_enabled": settings.BIGQUERY_ENABLED,
            "slack_enabled": settings.SLACK_ENABLED,
        })

    if path == "/process" and request.method == "POST":
        data = request.get_json(silent=True) or {}
        bucket = data.get("bucket", settings.IMPORT_BUCKET)
        blob_path = data.get("path")

        if not blob_path:
            return jsonify({"error": "Missing 'path' in request body"}), 400

        result = run_async(process_file(bucket, blob_path))
        status_code = 200 if result.get("success") or result.get("skipped") else 500

        return jsonify(result), status_code

    if path == "/upload" and request.method == "POST":
        # Upload file from Apps Script to GCS for parallel testing
        import base64

        data = request.get_json(silent=True) or {}
        category = data.get("category")
        filename = data.get("filename")
        content_b64 = data.get("content")

        if not category or not filename or not content_b64:
            return jsonify({
                "error": "Missing required fields: category, filename, content"
            }), 400

        try:
            # Decode base64 content
            content = base64.b64decode(content_b64)

            # Normalize category name to match config format
            # e.g., "BA DASH TIK" -> "BA Dash TIK"
            # Marketplace codes (LAZ, SHO, TIK, TOK, BSL) stay uppercase
            marketplace_codes = {"LAZ", "SHO", "TIK", "TOK", "BSL"}
            normalized_category = " ".join(
                word if word.upper() in marketplace_codes else word.title()
                for word in category.split()
            )

            # Build blob path: {category}/{filename}
            blob_path = f"{normalized_category}/{filename}"

            # Upload to GCS
            success = upload_blob(settings.IMPORT_BUCKET, blob_path, content)

            if success:
                logger.info(
                    "File uploaded from Apps Script",
                    category=category,
                    filename=filename,
                    size_bytes=len(content),
                )
                return jsonify({
                    "success": True,
                    "path": blob_path,
                    "bucket": settings.IMPORT_BUCKET,
                    "size_bytes": len(content),
                })
            else:
                return jsonify({"error": "Failed to upload to GCS"}), 500

        except Exception as e:
            logger.error(f"Upload failed: {e}", exc_info=True)
            return jsonify({"error": str(e)}), 500

    if path == "/sync" and request.method == "POST":
        # Sync Admin Sheet to BigQuery (called by Cloud Scheduler daily)
        from admin_sync import sync_all

        try:
            result = sync_all()
            success = len(result.get("errors", [])) == 0
            status_code = 200 if success else 500

            logger.info(
                "Admin sync completed",
                total_rows=result.get("total_rows"),
                errors=len(result.get("errors", [])),
            )

            return jsonify(result), status_code

        except Exception as e:
            logger.error(f"Sync failed: {e}", exc_info=True)
            return jsonify({"error": str(e)}), 500

    return jsonify({"error": "Not found"}), 404


# For local testing
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage: python main.py <bucket> <blob_path>")
        print("Example: python main.py ibot-v2-imports 'BA Produk LAZ/GS data.xlsx'")
        sys.exit(1)

    bucket = sys.argv[1]
    path = sys.argv[2]

    result = run_async(process_file(bucket, path))
    print(f"Result: {result}")
