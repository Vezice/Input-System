"""
iBot v2 Brand Detection (Legacy Mode)

Temporary module to detect brand from file content by matching product codes
against validation data in BigQuery (synced from Admin Sheet).

This is toggleable - set LEGACY_BRAND_DETECTION=true to enable.
Final version will use brand codes in filenames provided by agents.
"""

import os
from typing import Dict, Optional
from collections import Counter

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from utils.logger import get_logger

logger = get_logger(__name__)

# Feature flag - set to "true" to enable legacy brand detection
LEGACY_BRAND_DETECTION = os.getenv("LEGACY_BRAND_DETECTION", "true").lower() == "true"

# BigQuery configuration
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "fbi-dev-484410")
CONFIG_DATASET = "admin_config"

# Cache for validation mappings (product_id -> brand)
_validation_cache: Dict[str, Dict[str, str]] = {}
_bq_client = None


def _get_bq_client() -> bigquery.Client:
    """Get BigQuery client."""
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=PROJECT_ID)
    return _bq_client


def _get_validation_table_name(category_name: str) -> str:
    """Convert category name to validation table name."""
    return category_name.lower().replace(" ", "_") + "_validation"


def load_brand_validation(category_name: str) -> Dict[str, str]:
    """
    Load brand validation mapping from BigQuery.

    If BigQuery doesn't have the validation data, triggers a sync from
    Google Sheets first, then reads from BigQuery.

    The validation table has:
    - Column A: Brand/folder name
    - Column B: Product ID
    - Row 1 is the header row (skipped)

    Returns dict of {product_id: brand_code}
    """
    if category_name in _validation_cache:
        return _validation_cache[category_name]

    client = _get_bq_client()
    table_name = _get_validation_table_name(category_name)
    table_id = f"{PROJECT_ID}.{CONFIG_DATASET}.{table_name}"

    def _query_validation() -> Dict[str, str]:
        """Query validation data from BigQuery."""
        query = f"""
        SELECT A as brand, B as product_id
        FROM `{table_id}`
        WHERE A != 'Brand' AND A != 'Folder' AND A IS NOT NULL
          AND B IS NOT NULL AND TRIM(B) != ''
        """

        rows = client.query(query).result()

        mapping = {}
        for row in rows:
            brand = str(row.brand).strip() if row.brand else ""
            product_id = str(row.product_id).strip() if row.product_id else ""
            if product_id and brand:
                mapping[product_id] = brand

        return mapping

    try:
        mapping = _query_validation()

        if mapping:
            logger.info(f"Loaded {len(mapping)} brand mappings for {category_name} from BigQuery")
            _validation_cache[category_name] = mapping
            return mapping

        # Table exists but empty or only has header - try sync
        logger.info(f"Validation data empty for {category_name}, syncing from Sheets...")

    except NotFound:
        # Table doesn't exist - sync from Sheets first
        logger.info(f"Validation table not found for {category_name}, syncing from Sheets...")

    except Exception as e:
        logger.warning(f"Failed to query validation for {category_name}: {e}")
        _validation_cache[category_name] = {}
        return {}

    # Sync from Google Sheets
    try:
        from admin_sync import sync_validation
        sync_validation(category_name)

        # Now read from BigQuery
        mapping = _query_validation()
        logger.info(f"Loaded {len(mapping)} brand mappings for {category_name} after sync")
        _validation_cache[category_name] = mapping
        return mapping

    except Exception as e:
        logger.warning(f"Failed to sync/load validation for {category_name}: {e}")
        _validation_cache[category_name] = {}
        return {}


def detect_brand_from_data(
    parsed_file,
    category_name: str,
    num_rows_to_check: int = 30,
    match_threshold: float = 0.10
) -> Optional[str]:
    """
    Detect brand by matching product codes against validation data.

    Mimics iBot v1 logic:
    1. Read product codes from first column of data rows
    2. Match against validation data in BigQuery
    3. Return brand with most matches (if above threshold)

    Args:
        parsed_file: ParsedFile object with headers and rows
        category_name: Category name (e.g., "BA Produk LAZ")
        num_rows_to_check: Number of data rows to check
        match_threshold: Minimum match rate required (0.10 = 10%)

    Returns:
        Brand code string, or None if not detected
    """
    if not LEGACY_BRAND_DETECTION:
        return None

    if not parsed_file.rows:
        return None

    # Load validation mapping
    validation_map = load_brand_validation(category_name)
    if not validation_map:
        logger.warning(f"No validation mapping found for {category_name}")
        return None

    # Get first column name (product ID column)
    if not parsed_file.headers:
        return None

    first_col = parsed_file.headers[0]

    # Collect product codes from first N rows
    values_to_check = []
    for row in parsed_file.rows[:num_rows_to_check]:
        value = row.get(first_col, "")
        if value:
            # Convert to string and normalize - floats from Excel have ".0" suffix
            value_str = str(value).strip()
            if value_str.endswith(".0"):
                value_str = value_str[:-2]
            if value_str:
                values_to_check.append(value_str)

    if not values_to_check:
        return None

    # Match against validation and count by brand
    brand_counts = Counter()
    match_count = 0

    for value in values_to_check:
        if value in validation_map:
            brand = validation_map[value]
            brand_counts[brand] += 1
            match_count += 1

    # Check if we have enough matches
    required_matches = len(values_to_check) * match_threshold

    if match_count < required_matches or match_count == 0:
        logger.warning(
            f"Not enough matches for brand detection: {match_count}/{len(values_to_check)} "
            f"(need {required_matches:.1f})"
        )
        return None

    # Return brand with most matches
    brand, count = brand_counts.most_common(1)[0]
    logger.info(
        f"Detected brand '{brand}' from product codes "
        f"({count}/{match_count} matches, {len(values_to_check)} checked)"
    )

    return brand


def is_legacy_detection_enabled() -> bool:
    """Check if legacy brand detection is enabled."""
    return LEGACY_BRAND_DETECTION
