"""
iBot v2 Configuration

Fetches config dynamically from Admin Sheet (Google Sheets).
Config is loaded once on startup and cached for the function lifetime.

Admin Sheet: https://docs.google.com/spreadsheets/d/11aYJSWTW7xxZcyfREdcvGoUhgPnl4xU8MQM6lE1se4M
"""

import os
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any

from utils.logger import get_logger

logger = get_logger(__name__)

# Admin Sheet configuration
SPREADSHEET_ID = os.getenv("ADMIN_SHEET_ID", "11aYJSWTW7xxZcyfREdcvGoUhgPnl4xU8MQM6lE1se4M")
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
# Dynamic Config Loading from Admin Sheet
# ============================================================

_categories_cache: Optional[Dict[str, CategoryConfig]] = None
_sheets_service = None


def _get_sheets_service():
    """Get authenticated Google Sheets service (cached)."""
    global _sheets_service
    if _sheets_service is None:
        try:
            import google.auth
            from googleapiclient.discovery import build

            # Use Application Default Credentials (works in Cloud Functions)
            credentials, project = google.auth.default(scopes=SCOPES)
            _sheets_service = build("sheets", "v4", credentials=credentials)
            logger.info("Google Sheets service initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Sheets service: {e}")
            raise
    return _sheets_service


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

        # Only Central role (avoid worker duplicates)
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

        # Headers start at column 5 (index 4) - after Category, Header Row, Data Row, Filename Pattern
        required_headers = [h for h in row[4:] if h]

        # Parse category type and marketplace from name
        parts = category_name.rsplit(" ", 1)
        category_type = parts[0] if len(parts) == 2 else category_name
        marketplace = parts[1] if len(parts) == 2 else "UNKNOWN"

        # BigQuery table name
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


def load_categories() -> Dict[str, CategoryConfig]:
    """
    Load all categories from Admin Sheet.

    Called once on startup, cached for function lifetime.
    """
    global _categories_cache

    if _categories_cache is not None:
        return _categories_cache

    logger.info("Loading config from Admin Sheet...")

    try:
        # Fetch all sheets
        list_data = _fetch_sheet_data("List", "A1:I100")
        type_data = _fetch_sheet_data("Type Validation", "A1:Z50")
        alias_data = _fetch_sheet_data("Unique Column", "A1:D100")

        # Parse data
        webhooks = _parse_list_sheet(list_data)
        categories_raw = _parse_type_validation_sheet(type_data)
        aliases = _parse_unique_column_sheet(alias_data)

        # Build category configs
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

        logger.info(f"Loaded {len(categories)} categories from Admin Sheet")
        _categories_cache = categories
        return categories

    except Exception as e:
        import traceback
        logger.error(f"Failed to load config from Admin Sheet: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return {}


def get_categories() -> Dict[str, CategoryConfig]:
    """Get all categories (loads from Admin Sheet if not cached)."""
    return load_categories()


def get_category(name: str) -> Optional[CategoryConfig]:
    """Get category config by name (folder name in GCS)."""
    return get_categories().get(name)


def get_slack_webhook(category_name: str) -> str:
    """Get Slack webhook URL for a category."""
    category = get_category(category_name)
    if category and category.slack_webhook_url:
        return category.slack_webhook_url
    return settings.SLACK_WEBHOOK_URL


def reload_categories() -> Dict[str, CategoryConfig]:
    """Force reload categories from Admin Sheet."""
    global _categories_cache
    _categories_cache = None
    return load_categories()
