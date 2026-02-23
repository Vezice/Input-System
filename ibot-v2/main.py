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
from brand_detector import detect_brand_from_data, detect_brand_from_filename, is_legacy_detection_enabled
from utils.gcs_utils import download_blob_as_bytes, list_blobs_in_folder, move_to_archive, move_to_failed, upload_blob
from utils.logger import get_logger

logger = get_logger(__name__)


def extract_info_from_path(blob_path: str) -> tuple[Optional[str], Optional[str], str]:
    """
    Extract category, brand code, and filename from GCS blob path.

    Expected formats:
    - BA Produk: {CATEGORY}/{BRAND} filename.xlsx
      Example: BA Produk LAZ/GS 2024-01-15_data.xlsx -> brand="GS"
    - BA Dash: {CATEGORY}/{MARKETPLACE} {BRAND} Overview_...xlsx
      Example: BA Dash TIK/TIK GS Overview_My Business Performance_....xlsx -> brand="GS"

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

    # Extract brand code from filename
    words = filename.split(" ")

    if category_name.startswith("BA Dash") and len(words) >= 2:
        # BA Dash files: "{MARKETPLACE} {BRAND} Overview_..."
        # Brand is the second word (e.g., "TIK GS Overview..." -> "GS")
        brand_code = words[1]
    elif len(words) >= 1:
        # BA Produk files: "{BRAND} filename.xlsx"
        # Brand is the first word
        brand_code = words[0]
    else:
        brand_code = filename.split("_")[0]

    # Clean up brand code (remove extension if present)
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
        logger.error(
            f"FILE FAILED: Unknown category '{category_name}'. "
            f"The folder name does not match any configured category in config.py. "
            f"Check that the folder name in GCS matches exactly (case-sensitive).",
            failure_reason="UNKNOWN_CATEGORY",
            filename=filename,
            category_attempted=category_name,
            path=blob_path,
        )
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
        logger.error(
            f"FILE FAILED: Could not download file from GCS. "
            f"The file may have been deleted, moved, or there may be a permissions issue. "
            f"Check that the file exists at gs://{bucket_name}/{blob_path}.",
            failure_reason="DOWNLOAD_FAILED",
            filename=filename,
            category=category_name,
            bucket=bucket_name,
            path=blob_path,
        )
        move_to_failed(bucket_name, blob_path)
        return {"success": False, "error": "Failed to download file"}

    # Parse file
    parsed_file = parse_file(filename, file_content, category)

    if not parsed_file.is_valid:
        error_msg = parsed_file.errors[0] if parsed_file.errors else "No data found"
        all_errors = "; ".join(parsed_file.errors) if parsed_file.errors else "No data rows found after parsing"
        logger.error(
            f"FILE FAILED: Could not parse file content. {all_errors}. "
            f"This typically means: (1) the file is corrupted or not a valid Excel/CSV, "
            f"(2) the file is empty or has no data rows, "
            f"(3) the file format doesn't match the extension (e.g., JSON saved as .xls), or "
            f"(4) openpyxl/xlrd cannot read this specific Excel variant.",
            failure_reason="PARSE_FAILED",
            filename=filename,
            category=category_name,
            brand=brand_code,
            errors=all_errors,
            total_rows=parsed_file.total_rows,
            path=blob_path,
        )
        move_to_failed(bucket_name, blob_path)

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
        detected = False

        # Try filename-based detection first (e.g., Shopee shop name lookup)
        filename_brand = detect_brand_from_filename(filename, category_name)
        if filename_brand:
            logger.info(f"Using filename-detected brand '{filename_brand}' instead of '{brand_code}'")
            brand_code = filename_brand
            detected = True

        if not detected and is_legacy_detection_enabled():
            # Use v1-style brand detection from product codes
            detected_brand = detect_brand_from_data(parsed_file, category_name)
            if detected_brand:
                logger.info(f"Using detected brand '{detected_brand}' instead of '{brand_code}'")
                brand_code = detected_brand
                detected = True
            else:
                # Fallback: try to get brand from 'akun' column
                extracted_brand = extract_brand_from_data(parsed_file)
                if extracted_brand:
                    logger.info(f"Using extracted brand '{extracted_brand}' instead of '{brand_code}'")
                    brand_code = extracted_brand
                    detected = True

        # If brand code is still invalid after all detection attempts, fail the file
        if not detected and not is_valid_brand_code(brand_code):
            logger.error(
                f"FILE FAILED: Could not detect a valid brand code. "
                f"Filename extraction gave '{brand_code}' which is not all-uppercase. "
                f"All fallback detection methods also failed: "
                f"(1) filename pattern lookup (e.g., Shopee shop name) - no match, "
                f"(2) product code matching against validation data - no match or below threshold, "
                f"(3) 'akun' column extraction - column not found or empty. "
                f"To fix: either rename the file with a valid brand code prefix (e.g., 'GS filename.xlsx'), "
                f"or add the product codes/shop name to the validation sheet in Admin Sheet.",
                failure_reason="BRAND_DETECTION_FAILED",
                filename=filename,
                category=category_name,
                brand_attempted=brand_code,
                path=blob_path,
            )
            move_to_failed(bucket_name, blob_path)
            return {"success": False, "error": f"Could not detect valid brand code. Filename gave '{brand_code}' which is invalid."}

    # Generate import ID
    import_id = f"ibotv2_{category.bigquery_table}_{brand_code}_{uuid.uuid4().hex[:8]}"

    # Upload to BigQuery
    if settings.BIGQUERY_ENABLED:
        loader = get_loader()
        success, result = loader.upload(parsed_file, brand_code, import_id)

        if not success:
            error_msg = result.get("error", "BigQuery upload failed")
            bq_errors = result.get("errors", [])
            logger.error(
                f"FILE FAILED: BigQuery upload failed after parsing succeeded. "
                f"Error: {error_msg}. "
                f"This typically means: (1) schema mismatch - the table has columns that don't match the data, "
                f"(2) permissions issue - the service account cannot write to BigQuery, "
                f"(3) table creation race condition - another function created the table simultaneously, or "
                f"(4) load job timeout - too much data for a single load job.",
                failure_reason="BIGQUERY_UPLOAD_FAILED",
                filename=filename,
                category=category_name,
                brand=brand_code,
                import_id=import_id,
                table=category.bigquery_table,
                rows_attempted=len(parsed_file.rows),
                columns=len(parsed_file.headers),
                bq_errors=str(bq_errors[:3]) if bq_errors else "none",
                path=blob_path,
            )
            move_to_failed(bucket_name, blob_path)

            return {"success": False, "error": error_msg, "import_id": import_id}

        rows_inserted = result.get("rows_inserted", 0)
    else:
        logger.info("BigQuery disabled, skipping upload")
        rows_inserted = len(parsed_file.rows)
        result = {"rows_inserted": rows_inserted}

    # Move to archive
    move_to_archive(bucket_name, blob_path)

    duration = time.time() - start_time

    # Send success notification (disabled for testing)
    # notifier = get_notifier(get_slack_webhook(category_name))
    # await notifier.notify_success(parsed_file, brand_code, import_id, rows_inserted, duration)

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


# Categories that use batch processing (triggered by v1 when all files are ready)
# These are snapshot categories where we replace entire table
BATCH_CATEGORIES = ["BA Produk", "Informasi", "Export SKU", "Demografis", "Proyeksi"]


async def batch_process_category(category_name: str) -> dict:
    """
    Process all files in a category folder at once.

    Called by v1 (Apps Script) when all workers complete.
    This ensures v1 and v2 process the same data at the same time.

    Args:
        category_name: Category name (e.g., "BA Produk SHO")

    Returns:
        Result dict with success status and details
    """
    start_time = time.time()
    bucket_name = settings.IMPORT_BUCKET

    logger.info(f"Batch processing started", category=category_name)

    # 1. Get category config
    category = get_category(category_name)
    if not category:
        logger.error(
            f"BATCH FAILED: Unknown category '{category_name}'. "
            f"Not found in config.py. Check the category name is exact (case-sensitive).",
            failure_reason="UNKNOWN_CATEGORY",
            category_attempted=category_name,
        )
        return {"success": False, "error": f"Unknown category: {category_name}"}

    # 2. List all files in category folder
    blob_paths = list_blobs_in_folder(bucket_name, category_name)

    if not blob_paths:
        logger.info(f"No files to process in {category_name}")
        return {
            "success": True,
            "message": "No files to process",
            "category": category_name,
            "files_processed": 0,
            "rows_inserted": 0,
        }

    logger.info(f"Found {len(blob_paths)} files to process", category=category_name)

    # 3. Parse all files and accumulate rows
    all_rows = []
    all_headers = None
    processed_files = []
    failed_files = []

    for blob_path in blob_paths:
        filename = blob_path.split("/")[-1]

        # Download file
        file_content = download_blob_as_bytes(bucket_name, blob_path)
        if not file_content:
            logger.error(
                f"FILE FAILED (batch): Could not download file from GCS. "
                f"The file may have been deleted/moved or there is a permissions issue.",
                failure_reason="DOWNLOAD_FAILED",
                filename=filename,
                category=category_name,
                path=blob_path,
            )
            failed_files.append({"path": blob_path, "error": "Download failed"})
            move_to_failed(bucket_name, blob_path)
            continue

        # Parse file
        parsed = parse_file(filename, file_content, category)
        if not parsed.is_valid:
            error_msg = parsed.errors[0] if parsed.errors else "Parse failed"
            all_errors = "; ".join(parsed.errors) if parsed.errors else "No data rows"
            logger.error(
                f"FILE FAILED (batch): Could not parse file. {all_errors}. "
                f"Possible causes: corrupted file, wrong format, empty file, "
                f"or unsupported Excel variant (e.g., missing stylesheet, invalid XML).",
                failure_reason="PARSE_FAILED",
                filename=filename,
                category=category_name,
                errors=all_errors,
                total_rows=parsed.total_rows,
                path=blob_path,
            )
            failed_files.append({"path": blob_path, "error": error_msg})
            move_to_failed(bucket_name, blob_path)
            continue

        # Extract brand code
        _, brand_code, _ = extract_info_from_path(blob_path)

        # Validate brand code
        def is_valid_brand_code(code: str) -> bool:
            if not code:
                return False
            return code == code.upper()

        if not is_valid_brand_code(brand_code):
            # Try filename-based detection first (e.g., Shopee shop name lookup)
            filename_brand = detect_brand_from_filename(filename, category_name)
            if filename_brand:
                brand_code = filename_brand
            elif is_legacy_detection_enabled():
                detected_brand = detect_brand_from_data(parsed, category_name)
                if detected_brand:
                    brand_code = detected_brand
                else:
                    extracted_brand = extract_brand_from_data(parsed)
                    if extracted_brand:
                        brand_code = extracted_brand

            if not is_valid_brand_code(brand_code):
                logger.error(
                    f"FILE FAILED (batch): Invalid brand code '{brand_code}' - not all uppercase. "
                    f"All detection fallbacks failed: "
                    f"(1) filename pattern lookup - no match, "
                    f"(2) product code matching against validation - no match, "
                    f"(3) 'akun' column extraction - not found. "
                    f"Fix: rename file with valid brand prefix or update validation sheet.",
                    failure_reason="BRAND_DETECTION_FAILED",
                    filename=filename,
                    category=category_name,
                    brand_attempted=brand_code,
                    path=blob_path,
                )
                failed_files.append({"path": blob_path, "error": f"Invalid brand code: {brand_code}"})
                move_to_failed(bucket_name, blob_path)
                continue

        # Add brand code to each row and accumulate
        for row in parsed.rows:
            row["_brand_code"] = brand_code
        all_rows.extend(parsed.rows)

        # Use headers from first successful file
        if all_headers is None:
            all_headers = parsed.headers

        processed_files.append(blob_path)
        logger.info(f"Parsed {filename}: {len(parsed.rows)} rows, brand={brand_code}")

    if not all_rows:
        logger.error(
            f"BATCH FAILED: No valid data found in any of the {len(blob_paths)} files for {category_name}. "
            f"All {len(failed_files)} files failed individually. "
            f"Check the individual file failure logs above for specific reasons per file.",
            failure_reason="ALL_FILES_FAILED",
            category=category_name,
            total_files=len(blob_paths),
            failed_files=len(failed_files),
            failed_details=str([f["error"] for f in failed_files[:5]]),
        )
        return {
            "success": False,
            "error": "No valid data in any files",
            "category": category_name,
            "files_processed": 0,
            "files_failed": len(failed_files),
        }

    # 4. Delete entire table data (except header row)
    loader = get_loader()
    deleted = loader.delete_all_data(category.bigquery_table)
    logger.info(f"Deleted {deleted} existing rows from {category.bigquery_table}")

    # 5. Insert all rows using batch upload
    success, result = loader.batch_upload(all_rows, all_headers, category)

    if not success:
        error_msg = result.get("error", "Batch upload failed")
        bq_errors = result.get("errors", [])
        logger.error(
            f"BATCH FAILED: BigQuery batch upload failed for {category_name}. "
            f"Error: {error_msg}. "
            f"All {len(processed_files)} parsed files will be moved to failed/. "
            f"This typically means a schema mismatch, permissions issue, or load job error. "
            f"If schema mismatch: try dropping the table and re-processing.",
            failure_reason="BIGQUERY_BATCH_UPLOAD_FAILED",
            category=category_name,
            table=category.bigquery_table,
            files_count=len(processed_files),
            total_rows=len(all_rows),
            columns=len(all_headers) if all_headers else 0,
            bq_errors=str(bq_errors[:3]) if bq_errors else "none",
        )
        # Move all processed files to failed
        for blob_path in processed_files:
            move_to_failed(bucket_name, blob_path)
        return {
            "success": False,
            "error": error_msg,
            "category": category_name,
        }

    # 6. Archive all processed files
    for blob_path in processed_files:
        move_to_archive(bucket_name, blob_path)

    duration = time.time() - start_time
    rows_inserted = result.get("rows_inserted", 0)

    # Log summary with failure details if any files failed
    if failed_files:
        failed_summary = "; ".join(
            f"{f['path'].split('/')[-1]}: {f['error']}" for f in failed_files[:10]
        )
        logger.warning(
            f"BATCH PARTIAL: {len(processed_files)} files succeeded, {len(failed_files)} files failed in {category_name}. "
            f"Failed files and reasons: {failed_summary}",
            category=category_name,
            files_succeeded=len(processed_files),
            files_failed=len(failed_files),
            rows_inserted=rows_inserted,
        )

    logger.info(
        f"Batch processing complete",
        category=category_name,
        files=len(processed_files),
        failed=len(failed_files),
        rows=rows_inserted,
        duration=f"{duration:.1f}s",
    )

    return {
        "success": True,
        "category": category_name,
        "files_processed": len(processed_files),
        "files_failed": len(failed_files),
        "failed_details": [{"file": f["path"].split("/")[-1], "error": f["error"]} for f in failed_files],
        "rows_inserted": rows_inserted,
        "duration_seconds": round(duration, 1),
    }


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

    # Skip batch categories - they use /batch-process endpoint triggered by v1
    category_name = blob_path.split("/")[0] if "/" in blob_path else None
    if category_name and any(cat in category_name for cat in BATCH_CATEGORIES):
        logger.info(
            f"Skipping GCS trigger for batch category (will be processed via /batch-process)",
            category=category_name,
            path=blob_path,
        )
        return

    # Process the file (only for non-batch categories like BA Dash)
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
            # e.g., "ba produk laz" -> "BA Produk LAZ"
            # Keep uppercase: BA, marketplace codes (LAZ, SHO, TIK, TOK, BSL)
            keep_uppercase = {"BA", "LAZ", "SHO", "TIK", "TOK", "BSL"}
            normalized_category = " ".join(
                word.upper() if word.upper() in keep_uppercase else word.title()
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

    if path == "/batch-process" and request.method == "POST":
        # Batch process all files in a category (called by v1 when all workers complete)
        data = request.get_json(silent=True) or {}
        category_name = data.get("category")

        if not category_name:
            return jsonify({"error": "Missing 'category' in request body"}), 400

        try:
            result = run_async(batch_process_category(category_name))
            status_code = 200 if result.get("success") else 500

            logger.info(
                "Batch process completed",
                category=category_name,
                success=result.get("success"),
                files=result.get("files_processed"),
                rows=result.get("rows_inserted"),
            )

            return jsonify(result), status_code

        except Exception as e:
            logger.error(f"Batch process failed: {e}", exc_info=True)
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
