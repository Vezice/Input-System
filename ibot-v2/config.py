"""
iBot v2 Configuration

Fetches config dynamically from BigQuery (synced from Admin Sheet).
Falls back to Google Sheets if BigQuery data not available.

Admin Sheet: https://docs.google.com/spreadsheets/d/11aYJSWTW7xxZcyfREdcvGoUhgPnl4xU8MQM6lE1se4M
BigQuery: admin_config dataset
"""

import os
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any

from utils.logger import get_logger

logger = get_logger(__name__)

# Configuration sources
SPREADSHEET_ID = os.getenv("ADMIN_SHEET_ID", "11aYJSWTW7xxZcyfREdcvGoUhgPnl4xU8MQM6lE1se4M")
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "fbi-dev-484410")
CONFIG_DATASET = "admin_config"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


@dataclass
class CategoryConfig:
    """Configuration for a data category."""
    name: str
    category_type: str
    marketplace: str
    bigquery_table: str
    required_headers: List[str]
    slack_webhook_url: str = ""
    header_row: int = 0
    data_start_row: int = 1
    column_aliases: Dict[str, List[str]] = field(default_factory=dict)


class Settings:
    """Application settings from environment."""

    # Google Cloud
    PROJECT_ID: str = os.getenv("GOOGLE_CLOUD_PROJECT", "fbi-dev-484410")
    BIGQUERY_DATASET: str = os.getenv("BIGQUERY_DATASET", "ibot_v2_data")

    # Cloud Storage (asia-southeast2 / Jakarta)
    IMPORT_BUCKET: str = os.getenv("IMPORT_BUCKET", "ibot-v2-imports")

    # Default Slack (fallback)
    SLACK_WEBHOOK_URL: str = os.getenv("SLACK_WEBHOOK_URL", "")
    SLACK_CHANNEL: str = os.getenv("SLACK_CHANNEL", "#ibot-v2-notifications")
    SLACK_MENTION_USER: str = os.getenv("SLACK_MENTION_USER", "<@U0A6B24777X>")

    # Processing
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "500"))

    # Feature flags
    BIGQUERY_ENABLED: bool = os.getenv("BIGQUERY_ENABLED", "true").lower() == "true"
    SLACK_ENABLED: bool = os.getenv("SLACK_ENABLED", "true").lower() == "true"


settings = Settings()


# ============================================================
# Dynamic Config Loading - BigQuery First, Sheets Fallback
# ============================================================

_categories_cache: Optional[Dict[str, CategoryConfig]] = None
_sheets_service = None
_bq_client = None


def _get_bq_client():
    """Get BigQuery client (cached)."""
    global _bq_client
    if _bq_client is None:
        from google.cloud import bigquery
        _bq_client = bigquery.Client(project=PROJECT_ID)
    return _bq_client


def _get_sheets_service():
    """Get authenticated Google Sheets service (cached)."""
    global _sheets_service
    if _sheets_service is None:
        try:
            import google.auth
            from googleapiclient.discovery import build

            credentials, project = google.auth.default(scopes=SCOPES)
            _sheets_service = build("sheets", "v4", credentials=credentials)
            logger.info("Google Sheets service initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Sheets service: {e}")
            raise
    return _sheets_service


# ============================================================
# BigQuery Config Loading
# ============================================================

def _load_categories_from_bigquery() -> Optional[Dict[str, CategoryConfig]]:
    """
    Load categories from BigQuery admin_config.categories table.

    Table format (Excel-style columns):
    - Row 1: Headers (A=Category, B=Role, C=..., H=Slack Webhook URL, etc.)
    - Row 2+: Data

    Returns None if table doesn't exist or is empty.
    """
    try:
        from google.cloud.exceptions import NotFound

        client = _get_bq_client()
        table_id = f"{PROJECT_ID}.{CONFIG_DATASET}.categories"

        # Check if table exists
        try:
            client.get_table(table_id)
        except NotFound:
            logger.info("BigQuery categories table not found, will use Sheets")
            return None

        # Query all rows
        query = f"SELECT * FROM `{table_id}`"
        rows = list(client.query(query).result())

        if not rows:
            logger.info("BigQuery categories table is empty")
            return None

        # First row should be headers
        header_row = None
        data_rows = []
        for row in rows:
            row_dict = dict(row)
            # Check if this is the header row (A column contains header-like text)
            a_val = row_dict.get("A", "")
            if a_val and a_val.lower() in ["category", "kategori", "name"]:
                header_row = row_dict
            else:
                data_rows.append(row_dict)

        if not header_row:
            # Assume first row is header if not identified
            if rows:
                header_row = dict(rows[0])
                data_rows = [dict(r) for r in rows[1:]]

        # Find column indices for important fields
        # Headers are in columns A, B, C, etc.
        # We need: Category (A), Role (B), Slack Webhook URL (H typically)
        webhook_col = None
        for col in ["H", "I", "J", "K"]:
            val = header_row.get(col, "")
            if val and "slack" in val.lower() and "webhook" in val.lower():
                webhook_col = col
                break

        # Build categories from data rows
        categories = {}
        webhooks = {}

        for row in data_rows:
            category_name = row.get("A", "")
            role = row.get("B", "")
            webhook = row.get(webhook_col, "") if webhook_col else ""

            if not category_name:
                continue

            # Only Central role for webhooks (avoid worker duplicates)
            if role == "Central" and webhook:
                webhooks[category_name] = webhook

        # Now load Type Validation data
        type_configs = _load_type_validation_from_bigquery()

        if not type_configs:
            logger.info("No type validation data in BigQuery")
            return None

        # Merge with webhooks
        for cat in type_configs:
            name = cat["name"]
            categories[name] = CategoryConfig(
                name=name,
                category_type=cat["category_type"],
                marketplace=cat["marketplace"],
                bigquery_table=cat["bigquery_table"],
                required_headers=cat["required_headers"],
                slack_webhook_url=webhooks.get(name, ""),
                header_row=cat["header_row"],
                data_start_row=cat["data_start_row"],
                column_aliases={},
            )

        logger.info(f"Loaded {len(categories)} categories from BigQuery")
        return categories

    except Exception as e:
        logger.warning(f"Failed to load from BigQuery: {e}")
        return None


def _load_type_validation_from_bigquery() -> List[Dict[str, Any]]:
    """Load type validation config from BigQuery.

    Handles multiple rows per category (e.g., Indonesian + English headers)
    by combining all header variants into a single config.
    """
    try:
        from google.cloud.exceptions import NotFound

        client = _get_bq_client()
        table_id = f"{PROJECT_ID}.{CONFIG_DATASET}.type_validation"

        try:
            client.get_table(table_id)
        except NotFound:
            return []

        query = f"SELECT * FROM `{table_id}`"
        rows = list(client.query(query).result())

        if not rows:
            return []

        # Group rows by category name to combine header variants
        category_data: Dict[str, Dict[str, Any]] = {}
        is_first = True

        for row in rows:
            row_dict = dict(row)
            a_val = row_dict.get("A", "")

            # Skip header row
            if is_first or (a_val and a_val.lower() in ["category", "kategori"]):
                is_first = False
                continue

            if not a_val:
                continue

            category_name = a_val

            # Extract headers from this row (columns E onwards)
            row_headers = []
            for col in ["E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T"]:
                val = row_dict.get(col, "")
                if val:
                    row_headers.append(val)

            if category_name not in category_data:
                # First row for this category - initialize
                header_row = int(row_dict.get("B", 1)) - 1 if row_dict.get("B") else 0
                data_row = int(row_dict.get("C", 2)) - 1 if row_dict.get("C") else 1

                parts = category_name.rsplit(" ", 1)
                category_type = parts[0] if len(parts) == 2 else category_name
                marketplace = parts[1] if len(parts) == 2 else "UNKNOWN"
                bq_table = category_name.lower().replace(" ", "_")

                category_data[category_name] = {
                    "name": category_name,
                    "category_type": category_type,
                    "marketplace": marketplace,
                    "bigquery_table": bq_table,
                    "header_row": header_row,
                    "data_start_row": data_row,
                    "required_headers": set(row_headers),  # Use set to avoid duplicates
                }
            else:
                # Additional row (e.g., different language) - merge headers
                category_data[category_name]["required_headers"].update(row_headers)

        # Convert sets back to lists
        categories = []
        for cat_data in category_data.values():
            cat_data["required_headers"] = list(cat_data["required_headers"])
            categories.append(cat_data)

        return categories

    except Exception as e:
        logger.warning(f"Failed to load type validation from BigQuery: {e}")
        return []


# ============================================================
# Google Sheets Config Loading (Fallback)
# ============================================================

def _fetch_sheet_data(sheet_name: str, range_str: str = "A1:Z100") -> List[List[str]]:
    """Fetch data from a specific sheet."""
    service = _get_sheets_service()
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{sheet_name}!{range_str}"
    ).execute()
    return result.get("values", [])


def _parse_list_sheet(data: List[List[str]]) -> Dict[str, str]:
    """Parse List sheet for Slack webhook URLs."""
    webhooks = {}
    if not data or len(data) < 2:
        return webhooks

    headers = data[0]
    webhook_idx = None
    for i, h in enumerate(headers):
        if "slack" in h.lower() and "webhook" in h.lower():
            webhook_idx = i
            break

    if webhook_idx is None:
        return webhooks

    for row in data[1:]:
        if len(row) <= webhook_idx:
            continue
        category = row[0] if row else ""
        role = row[1] if len(row) > 1 else ""
        webhook = row[webhook_idx] if len(row) > webhook_idx else ""

        if category and role == "Central" and webhook:
            webhooks[category] = webhook

    return webhooks


def _parse_type_validation_sheet(data: List[List[str]]) -> List[Dict[str, Any]]:
    """Parse Type Validation sheet for category configs."""
    categories = []
    if not data or len(data) < 2:
        return categories

    for row in data[1:]:
        if not row or not row[0]:
            continue

        category_name = row[0]
        header_row = int(row[1]) - 1 if len(row) > 1 and row[1] else 0
        data_row = int(row[2]) - 1 if len(row) > 2 and row[2] else 1
        required_headers = [h for h in row[4:] if h]

        parts = category_name.rsplit(" ", 1)
        category_type = parts[0] if len(parts) == 2 else category_name
        marketplace = parts[1] if len(parts) == 2 else "UNKNOWN"
        bq_table = category_name.lower().replace(" ", "_")

        categories.append({
            "name": category_name,
            "category_type": category_type,
            "marketplace": marketplace,
            "bigquery_table": bq_table,
            "header_row": header_row,
            "data_start_row": data_row,
            "required_headers": required_headers,
        })

    return categories


def _parse_unique_column_sheet(data: List[List[str]]) -> Dict[str, Dict[str, List[str]]]:
    """Parse Unique Column sheet for column aliases."""
    aliases: Dict[str, Dict[str, List[str]]] = {}
    if not data or len(data) < 2:
        return aliases

    for row in data[1:]:
        if len(row) < 4 or not row[0]:
            continue

        category_name = row[0]
        standard_column = row[1]
        pattern = row[3] if len(row) > 3 else ""

        if not pattern:
            continue

        if category_name not in aliases:
            aliases[category_name] = {}
        if standard_column not in aliases[category_name]:
            aliases[category_name][standard_column] = []

        aliases[category_name][standard_column].append(pattern)

    return aliases


def _load_categories_from_sheets() -> Dict[str, CategoryConfig]:
    """Load categories from Google Sheets (fallback)."""
    logger.info("Loading config from Google Sheets (fallback)...")

    list_data = _fetch_sheet_data("List", "A1:I100")
    type_data = _fetch_sheet_data("Type Validation", "A1:Z50")
    alias_data = _fetch_sheet_data("Unique Column", "A1:D100")

    webhooks = _parse_list_sheet(list_data)
    categories_raw = _parse_type_validation_sheet(type_data)
    aliases = _parse_unique_column_sheet(alias_data)

    categories = {}
    for cat in categories_raw:
        name = cat["name"]
        categories[name] = CategoryConfig(
            name=name,
            category_type=cat["category_type"],
            marketplace=cat["marketplace"],
            bigquery_table=cat["bigquery_table"],
            required_headers=cat["required_headers"],
            slack_webhook_url=webhooks.get(name, ""),
            header_row=cat["header_row"],
            data_start_row=cat["data_start_row"],
            column_aliases=aliases.get(name, {}),
        )

    logger.info(f"Loaded {len(categories)} categories from Google Sheets")
    return categories


# ============================================================
# Main Config Loading (BigQuery, sync from Sheets if missing)
# ============================================================

def load_categories() -> Dict[str, CategoryConfig]:
    """
    Load all categories from BigQuery.

    If BigQuery doesn't have the data, triggers a sync from Google Sheets
    to BigQuery first, then reads from BigQuery.

    Called once on startup, cached for function lifetime.
    """
    global _categories_cache

    if _categories_cache is not None:
        return _categories_cache

    logger.info("Loading config from BigQuery...")

    try:
        # Try BigQuery first
        categories = _load_categories_from_bigquery()

        if categories:
            _categories_cache = categories
            return categories

        # BigQuery doesn't have data - sync from Sheets first
        logger.info("BigQuery config not found, syncing from Google Sheets...")
        from admin_sync import sync_categories, sync_type_validation

        sync_categories()
        sync_type_validation()

        # Now read from BigQuery
        categories = _load_categories_from_bigquery()

        if categories:
            _categories_cache = categories
            return categories

        # If still no data, something is wrong
        logger.error("Failed to load config after sync")
        return {}

    except Exception as e:
        import traceback
        logger.error(f"Failed to load config: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return {}


def get_categories() -> Dict[str, CategoryConfig]:
    """Get all categories (loads if not cached)."""
    return load_categories()


def get_category(name: str) -> Optional[CategoryConfig]:
    """Get category config by name (folder name in GCS).

    Case-insensitive lookup to handle variations like
    'BA DASH TIK' vs 'BA Dash TIK'.
    """
    categories = get_categories()

    # Try exact match first
    if name in categories:
        return categories[name]

    # Try case-insensitive match
    name_lower = name.lower()
    for cat_name, config in categories.items():
        if cat_name.lower() == name_lower:
            return config

    return None


def get_slack_webhook(category_name: str) -> str:
    """Get Slack webhook URL for a category."""
    category = get_category(category_name)
    if category and category.slack_webhook_url:
        return category.slack_webhook_url
    return settings.SLACK_WEBHOOK_URL


def reload_categories() -> Dict[str, CategoryConfig]:
    """Force reload categories."""
    global _categories_cache
    _categories_cache = None
    return load_categories()
