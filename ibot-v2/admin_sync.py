"""
Admin Sheet to BigQuery Sync

Syncs configuration and validation data from Admin Sheet to BigQuery.
This eliminates Google Sheets API rate limiting issues during file processing.

Tables created:
- admin_config.categories: Category configurations (from List sheet)
- admin_config.{category}_validation: Brand validation mappings per category

All columns are STRING type to preserve original data.
"""

import os
from typing import Dict, List, Optional, Tuple
from datetime import datetime

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from utils.logger import get_logger

logger = get_logger(__name__)

# Configuration
ADMIN_SHEET_ID = os.getenv("ADMIN_SHEET_ID", "11aYJSWTW7xxZcyfREdcvGoUhgPnl4xU8MQM6lE1se4M")
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "fbi-dev-484410")
CONFIG_DATASET = "admin_config"

# Validation sheet names per category
VALIDATION_SHEETS = [
    "BA Produk LAZ",
    "BA Produk SHO",
    "BA Produk TIK",
    "BA Dash LAZ",
    "BA Dash SHO",
    "BA Dash TIK",
    "BA Dash TOK",
    "Informasi Dasar SHO",
    "Informasi Dikirim SHO",
    "Informasi Media SHO",
    "Informasi Penjualan SHO",
    "Export SKU LAZ",
    "Export SKU TIK",
    "Demografis BSL",
    "Proyeksi Stok BSL",
]

_sheets_service = None
_bq_client = None


def _get_sheets_service():
    """Get authenticated Google Sheets service."""
    global _sheets_service
    if _sheets_service is None:
        try:
            import google.auth
            from googleapiclient.discovery import build

            credentials, _ = google.auth.default(
                scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
            )
            _sheets_service = build("sheets", "v4", credentials=credentials)
        except Exception as e:
            logger.error(f"Failed to init Sheets service: {e}")
            return None
    return _sheets_service


def _get_bq_client() -> bigquery.Client:
    """Get BigQuery client."""
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=PROJECT_ID)
    return _bq_client


def _ensure_dataset_exists():
    """Create admin_config dataset if it doesn't exist."""
    client = _get_bq_client()
    dataset_ref = bigquery.DatasetReference(PROJECT_ID, CONFIG_DATASET)

    try:
        client.get_dataset(dataset_ref)
        logger.debug(f"Dataset {CONFIG_DATASET} already exists")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "asia-southeast2"
        dataset.description = "Admin configuration synced from Google Sheets"
        client.create_dataset(dataset)
        logger.info(f"Created dataset {CONFIG_DATASET}")


def _fetch_sheet_data(sheet_name: str, range_spec: str) -> List[List[str]]:
    """Fetch data from a sheet."""
    service = _get_sheets_service()
    if not service:
        return []

    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=ADMIN_SHEET_ID,
            range=f"'{sheet_name}'!{range_spec}"
        ).execute()
        return result.get("values", [])
    except Exception as e:
        logger.warning(f"Failed to fetch {sheet_name}: {e}")
        return []


def _table_exists(table_id: str) -> bool:
    """Check if a table exists."""
    client = _get_bq_client()
    try:
        client.get_table(table_id)
        return True
    except NotFound:
        return False


def _drop_and_create_table(table_id: str, schema: List[bigquery.SchemaField]):
    """Drop table if exists and create new one."""
    client = _get_bq_client()

    # Drop if exists
    try:
        client.delete_table(table_id)
        logger.info(f"Dropped existing table {table_id}")
    except NotFound:
        pass

    # Create new table
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table)
    logger.info(f"Created table {table_id}")


def sync_categories() -> Tuple[bool, int]:
    """
    Sync category configuration from List sheet to BigQuery.

    Returns:
        Tuple of (success, rows_synced)
    """
    logger.info("Syncing categories from Admin Sheet...")

    # Fetch List sheet data
    rows = _fetch_sheet_data("List", "A1:I100")
    if not rows:
        logger.error("No data found in List sheet")
        return False, 0

    # First row is headers
    headers = rows[0] if rows else []
    data_rows = rows[1:] if len(rows) > 1 else []

    if not data_rows:
        logger.warning("No category data found")
        return False, 0

    _ensure_dataset_exists()

    # Create schema - all STRING columns using Excel-style naming
    # Row 1 will contain the original headers
    max_cols = max(len(row) for row in rows)
    schema = [
        bigquery.SchemaField(_get_column_name(i), "STRING", mode="NULLABLE")
        for i in range(max_cols)
    ]

    table_id = f"{PROJECT_ID}.{CONFIG_DATASET}.categories"
    _drop_and_create_table(table_id, schema)

    # Prepare rows - include headers as Row 1
    prepared_rows = []

    # Add header row
    header_row = {_get_column_name(i): str(v) if v else "" for i, v in enumerate(headers)}
    prepared_rows.append(header_row)

    # Add data rows
    for row in data_rows:
        prepared_row = {_get_column_name(i): str(v) if v else "" for i, v in enumerate(row)}
        prepared_rows.append(prepared_row)

    # Insert rows
    client = _get_bq_client()
    errors = client.insert_rows_json(table_id, prepared_rows)

    if errors:
        logger.error(f"Failed to insert categories: {errors}")
        return False, 0

    logger.info(f"Synced {len(prepared_rows)} category rows (including header)")
    return True, len(prepared_rows)


def sync_validation(category_name: str) -> Tuple[bool, int]:
    """
    Sync validation mapping for a category from Admin Sheet to BigQuery.

    The validation sheet has:
    - Column A: Brand/folder name
    - Column B: Product ID

    Returns:
        Tuple of (success, rows_synced)
    """
    sheet_name = f"{category_name} Validation"
    logger.info(f"Syncing validation: {sheet_name}")

    # Fetch validation data (including header) - up to 50k rows
    rows = _fetch_sheet_data(sheet_name, "A1:B50000")
    if not rows:
        logger.warning(f"No validation data found for {category_name}")
        return False, 0

    _ensure_dataset_exists()

    # Schema: A (brand), B (product_id) - all STRING
    schema = [
        bigquery.SchemaField("A", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("B", "STRING", mode="NULLABLE"),
    ]

    # Table name: sanitize category name
    table_name = category_name.lower().replace(" ", "_") + "_validation"
    table_id = f"{PROJECT_ID}.{CONFIG_DATASET}.{table_name}"
    _drop_and_create_table(table_id, schema)

    # Prepare rows (all rows including header)
    prepared_rows = []
    for row in rows:
        prepared_row = {
            "A": str(row[0]).strip() if len(row) > 0 and row[0] else "",
            "B": str(row[1]).strip() if len(row) > 1 and row[1] else "",
        }
        prepared_rows.append(prepared_row)

    # Insert rows
    client = _get_bq_client()

    # Insert in batches
    batch_size = 500
    total_inserted = 0
    for i in range(0, len(prepared_rows), batch_size):
        batch = prepared_rows[i:i + batch_size]
        errors = client.insert_rows_json(table_id, batch)
        if errors:
            logger.error(f"Insert errors for {category_name}: {errors[:3]}")
        else:
            total_inserted += len(batch)

    logger.info(f"Synced {total_inserted} validation rows for {category_name}")
    return True, total_inserted


def sync_type_validation() -> Tuple[bool, int]:
    """
    Sync Type Validation sheet from Admin Sheet to BigQuery.

    Returns:
        Tuple of (success, rows_synced)
    """
    logger.info("Syncing Type Validation from Admin Sheet...")

    rows = _fetch_sheet_data("Type Validation", "A1:Z50")
    if not rows:
        logger.error("No data found in Type Validation sheet")
        return False, 0

    _ensure_dataset_exists()

    # Create schema - all STRING columns using Excel-style naming
    max_cols = max(len(row) for row in rows)
    schema = [
        bigquery.SchemaField(_get_column_name(i), "STRING", mode="NULLABLE")
        for i in range(max_cols)
    ]

    table_id = f"{PROJECT_ID}.{CONFIG_DATASET}.type_validation"
    _drop_and_create_table(table_id, schema)

    # Prepare all rows (including header as Row 1)
    prepared_rows = []
    for row in rows:
        prepared_row = {_get_column_name(i): str(v) if v else "" for i, v in enumerate(row)}
        prepared_rows.append(prepared_row)

    # Insert rows
    client = _get_bq_client()
    errors = client.insert_rows_json(table_id, prepared_rows)

    if errors:
        logger.error(f"Failed to insert type validation: {errors}")
        return False, 0

    logger.info(f"Synced {len(prepared_rows)} type validation rows")
    return True, len(prepared_rows)


def sync_all() -> Dict[str, any]:
    """
    Sync all configuration and validation data from Admin Sheet to BigQuery.

    Returns:
        Summary of sync results
    """
    logger.info("Starting full Admin Sheet sync to BigQuery...")
    start_time = datetime.now()

    results = {
        "categories": {"success": False, "rows": 0},
        "type_validation": {"success": False, "rows": 0},
        "validations": {},
        "total_rows": 0,
        "errors": [],
    }

    # Sync categories (List sheet)
    success, rows = sync_categories()
    results["categories"] = {"success": success, "rows": rows}
    results["total_rows"] += rows

    # Sync Type Validation sheet
    success, rows = sync_type_validation()
    results["type_validation"] = {"success": success, "rows": rows}
    results["total_rows"] += rows

    # Sync all validation sheets
    for category in VALIDATION_SHEETS:
        try:
            success, rows = sync_validation(category)
            results["validations"][category] = {"success": success, "rows": rows}
            results["total_rows"] += rows
        except Exception as e:
            error_msg = f"{category}: {str(e)}"
            results["errors"].append(error_msg)
            results["validations"][category] = {"success": False, "rows": 0, "error": str(e)}
            logger.error(f"Failed to sync {category}: {e}")

    duration = (datetime.now() - start_time).total_seconds()
    results["duration_seconds"] = duration

    logger.info(
        f"Admin sync complete: {results['total_rows']} total rows in {duration:.1f}s"
    )

    return results


def _get_column_name(index: int) -> str:
    """
    Convert column index to Excel-style column name.
    0 -> A, 1 -> B, ..., 25 -> Z, 26 -> AA, 27 -> AB, etc.
    """
    result = ""
    while True:
        result = chr(ord('A') + (index % 26)) + result
        index = index // 26 - 1
        if index < 0:
            break
    return result


# For testing
if __name__ == "__main__":
    import json
    results = sync_all()
    print(json.dumps(results, indent=2, default=str))
