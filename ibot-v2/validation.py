"""
iBot v2 Data Validation Module

Provides post-import validation:
- Duplicate detection and removal
- Missing brand detection
- Validation reporting to Slack

Similar to v1's AHA_ValidateCategoryBrands3 and AHA_ValidateBADashCategory3.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from google.cloud import bigquery

from config import settings
from slack_notifier import get_notifier
from utils.logger import get_logger

logger = get_logger(__name__)

# BigQuery client
_bq_client: Optional[bigquery.Client] = None


def get_bq_client() -> bigquery.Client:
    """Get or create BigQuery client."""
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=settings.PROJECT_ID)
    return _bq_client


# Table configurations for validation
TABLE_CONFIG = {
    "ba_produk_sho": {
        "date_column": None,  # No date column, validate by brand only
        "brand_column": "_brand_code",
        "unique_key": ["_brand_code", "A"],  # brand + product code
        "marketplace": "SHO",
    },
    "ba_produk_laz": {
        "date_column": None,
        "brand_column": "_brand_code",
        "unique_key": ["_brand_code", "A"],  # brand + product code
        "marketplace": "LAZ",
    },
    "ba_produk_tik": {
        "date_column": None,
        "brand_column": "_brand_code",
        "unique_key": ["_brand_code", "A"],
        "marketplace": "TIK",
    },
    "ba_dash_sho": {
        "date_column": "R",  # Column R has date
        "brand_column": "_brand_code",
        "unique_key": ["_brand_code", "R"],  # brand + date
        "marketplace": "SHO",
    },
    "ba_dash_laz": {
        "date_column": "A",  # Column A has date
        "brand_column": "_brand_code",
        "unique_key": ["_brand_code", "A"],
        "marketplace": "LAZ",
    },
    "ba_dash_tik": {
        "date_column": "A",
        "brand_column": "_brand_code",
        "unique_key": ["_brand_code", "A"],
        "marketplace": "TIK",
    },
    "ba_dash_tok": {
        "date_column": "A",
        "brand_column": "_brand_code",
        "unique_key": ["_brand_code", "A"],
        "marketplace": "TOK",
    },
}


def detect_duplicates(table_name: str) -> Dict:
    """
    Detect duplicate rows in a table based on its unique key configuration.

    Args:
        table_name: Name of the table (e.g., 'ba_dash_tok')

    Returns:
        Dict with duplicate info: {
            'total_rows': int,
            'unique_rows': int,
            'duplicate_count': int,
            'duplicates': List of {key, count}
        }
    """
    config = TABLE_CONFIG.get(table_name)
    if not config:
        logger.warning(f"No config for table {table_name}")
        return {"error": f"Unknown table: {table_name}"}

    client = get_bq_client()
    full_table = f"{settings.PROJECT_ID}.{settings.BIGQUERY_DATASET}.{table_name}"
    unique_key = config["unique_key"]

    # Build query to find duplicates
    key_columns = ", ".join(unique_key)

    query = f"""
    WITH duplicate_check AS (
        SELECT {key_columns}, COUNT(*) as cnt
        FROM `{full_table}`
        WHERE {config['brand_column']} != '_header_'
        GROUP BY {key_columns}
        HAVING COUNT(*) > 1
    )
    SELECT
        (SELECT COUNT(*) FROM `{full_table}` WHERE {config['brand_column']} != '_header_') as total_rows,
        (SELECT COUNT(*) FROM duplicate_check) as duplicate_combinations,
        (SELECT SUM(cnt) - COUNT(*) FROM duplicate_check) as extra_rows,
        ARRAY_AGG(STRUCT({key_columns}, cnt) ORDER BY cnt DESC LIMIT 10) as top_duplicates
    FROM duplicate_check
    """

    try:
        result = client.query(query).result()
        row = list(result)[0]

        total_rows = row.total_rows or 0
        dup_combos = row.duplicate_combinations or 0
        extra_rows = row.extra_rows or 0
        top_dups = row.top_duplicates or []

        return {
            "table": table_name,
            "total_rows": total_rows,
            "unique_rows": total_rows - extra_rows,
            "duplicate_combinations": dup_combos,
            "extra_rows": extra_rows,
            "top_duplicates": [dict(d) for d in top_dups] if top_dups else [],
        }
    except Exception as e:
        logger.error(f"Error detecting duplicates in {table_name}: {e}")
        return {"error": str(e)}


def remove_duplicates(table_name: str, dry_run: bool = True) -> Dict:
    """
    Remove duplicate rows from a table, keeping only distinct rows.

    Args:
        table_name: Name of the table
        dry_run: If True, only report what would be removed without actually removing

    Returns:
        Dict with removal info
    """
    config = TABLE_CONFIG.get(table_name)
    if not config:
        return {"error": f"Unknown table: {table_name}"}

    client = get_bq_client()
    full_table = f"{settings.PROJECT_ID}.{settings.BIGQUERY_DATASET}.{table_name}"

    # First detect duplicates
    dup_info = detect_duplicates(table_name)
    if "error" in dup_info:
        return dup_info

    if dup_info["extra_rows"] == 0:
        return {
            "table": table_name,
            "action": "none",
            "message": "No duplicates found",
            "rows_removed": 0,
        }

    if dry_run:
        return {
            "table": table_name,
            "action": "dry_run",
            "would_remove": dup_info["extra_rows"],
            "duplicate_combinations": dup_info["duplicate_combinations"],
        }

    # Use a temp table approach to preserve clustering:
    # 1. Create temp table with distinct rows
    # 2. Delete all from original
    # 3. Insert from temp
    # 4. Drop temp
    temp_table = f"{full_table}_dedup_temp"

    try:
        # Step 1: Create temp table with distinct rows
        create_temp_query = f"""
        CREATE TABLE `{temp_table}` AS
        SELECT DISTINCT *
        FROM `{full_table}`
        """
        job = client.query(create_temp_query)
        job.result()

        # Step 2: Delete all from original
        delete_query = f"DELETE FROM `{full_table}` WHERE TRUE"
        job = client.query(delete_query)
        job.result()

        # Step 3: Insert from temp
        insert_query = f"INSERT INTO `{full_table}` SELECT * FROM `{temp_table}`"
        job = client.query(insert_query)
        job.result()

        # Step 4: Drop temp table
        drop_query = f"DROP TABLE `{temp_table}`"
        job = client.query(drop_query)
        job.result()

        # Get new row count
        count_query = f"SELECT COUNT(*) as cnt FROM `{full_table}`"
        new_count = list(client.query(count_query).result())[0].cnt

        rows_removed = dup_info["total_rows"] - new_count

        logger.info(f"Removed {rows_removed} duplicate rows from {table_name}")

        return {
            "table": table_name,
            "action": "deduplicated",
            "rows_before": dup_info["total_rows"],
            "rows_after": new_count,
            "rows_removed": rows_removed,
        }
    except Exception as e:
        logger.error(f"Error removing duplicates from {table_name}: {e}")
        # Try to clean up temp table if it exists
        try:
            client.query(f"DROP TABLE IF EXISTS `{temp_table}`").result()
        except:
            pass
        return {"error": str(e)}


def remove_header_rows(table_name: str, dry_run: bool = True) -> Dict:
    """
    Remove rows where _brand_code = '_header_' (header rows inserted as data).

    Args:
        table_name: Name of the table
        dry_run: If True, only report what would be removed

    Returns:
        Dict with removal info
    """
    client = get_bq_client()
    full_table = f"{settings.PROJECT_ID}.{settings.BIGQUERY_DATASET}.{table_name}"

    # Count header rows
    count_query = f"""
    SELECT COUNT(*) as cnt
    FROM `{full_table}`
    WHERE _brand_code = '_header_'
    """

    try:
        header_count = list(client.query(count_query).result())[0].cnt

        if header_count == 0:
            return {
                "table": table_name,
                "action": "none",
                "message": "No header rows found",
                "rows_removed": 0,
            }

        if dry_run:
            return {
                "table": table_name,
                "action": "dry_run",
                "header_rows_found": header_count,
            }

        # Remove header rows
        delete_query = f"""
        DELETE FROM `{full_table}`
        WHERE _brand_code = '_header_'
        """

        job = client.query(delete_query)
        job.result()

        logger.info(f"Removed {header_count} header rows from {table_name}")

        return {
            "table": table_name,
            "action": "cleaned",
            "rows_removed": header_count,
        }
    except Exception as e:
        logger.error(f"Error removing header rows from {table_name}: {e}")
        return {"error": str(e)}


def get_expected_brands(table_name: str) -> List[str]:
    """
    Get expected brands for a table from Admin config validation tables.

    Args:
        table_name: Name of the data table (e.g., 'ba_dash_tok')

    Returns:
        List of expected brand codes
    """
    client = get_bq_client()

    # Validation tables are in admin_config dataset with _validation suffix
    validation_table = f"{settings.PROJECT_ID}.admin_config.{table_name}_validation"

    # Column B contains the brand codes (Kode Akun)
    query = f"""
    SELECT DISTINCT B as brand_code
    FROM `{validation_table}`
    WHERE B IS NOT NULL
      AND B != ''
      AND B != 'Kode Akun'  -- Skip header row
      AND NOT STARTS_WITH(B, '(')
    ORDER BY B
    """

    try:
        result = client.query(query).result()
        return [row.brand_code for row in result]
    except Exception as e:
        logger.warning(f"Could not get expected brands for {table_name}: {e}")
        return []


def validate_brands(table_name: str) -> Dict:
    """
    Validate imported brands against expected brands from Admin Sheet.

    Args:
        table_name: Name of the table to validate

    Returns:
        Dict with validation results
    """
    config = TABLE_CONFIG.get(table_name)
    if not config:
        return {"error": f"Unknown table: {table_name}"}

    marketplace = config["marketplace"]
    expected_brands = get_expected_brands(table_name)

    if not expected_brands:
        return {
            "table": table_name,
            "marketplace": marketplace,
            "warning": f"No expected brands found in {table_name}_validation table",
        }

    client = get_bq_client()
    full_table = f"{settings.PROJECT_ID}.{settings.BIGQUERY_DATASET}.{table_name}"

    # Get imported brands
    query = f"""
    SELECT DISTINCT UPPER(_brand_code) as brand
    FROM `{full_table}`
    WHERE _brand_code IS NOT NULL
      AND _brand_code != ''
      AND _brand_code != '_header_'
    """

    try:
        result = client.query(query).result()
        imported_brands = {row.brand for row in result}

        expected_set = {b.upper() for b in expected_brands}

        missing_brands = sorted(expected_set - imported_brands)
        extra_brands = sorted(imported_brands - expected_set)
        found_brands = sorted(imported_brands & expected_set)

        return {
            "table": table_name,
            "marketplace": marketplace,
            "expected_count": len(expected_brands),
            "found_count": len(found_brands),
            "missing_count": len(missing_brands),
            "extra_count": len(extra_brands),
            "missing_brands": missing_brands,
            "extra_brands": extra_brands[:10],  # Limit to 10
        }
    except Exception as e:
        logger.error(f"Error validating brands in {table_name}: {e}")
        return {"error": str(e)}


def validate_ba_dash_dates(table_name: str, days: int = 7) -> Dict:
    """
    Validate BA Dash data for the last N days, checking for missing brands per date.

    Args:
        table_name: BA Dash table name
        days: Number of days to check (default 7)

    Returns:
        Dict with daily validation results
    """
    config = TABLE_CONFIG.get(table_name)
    if not config or not config.get("date_column"):
        return {"error": f"Table {table_name} doesn't support date validation"}

    marketplace = config["marketplace"]
    date_col = config["date_column"]
    expected_brands = get_expected_brands(table_name)

    if not expected_brands:
        return {"warning": f"No expected brands found in {table_name}_validation table"}

    client = get_bq_client()
    full_table = f"{settings.PROJECT_ID}.{settings.BIGQUERY_DATASET}.{table_name}"

    # Get brands by date for last N days
    query = f"""
    SELECT
        CAST({date_col} AS STRING) as date_str,
        ARRAY_AGG(DISTINCT UPPER(_brand_code)) as brands
    FROM `{full_table}`
    WHERE _brand_code != '_header_'
      AND {date_col} IS NOT NULL
      AND SAFE_CAST({date_col} AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
    GROUP BY date_str
    ORDER BY date_str DESC
    """

    try:
        result = client.query(query).result()
        expected_set = {b.upper() for b in expected_brands}

        date_results = []
        for row in result:
            imported = set(row.brands) if row.brands else set()
            missing = sorted(expected_set - imported)

            date_results.append({
                "date": row.date_str,
                "found_count": len(imported),
                "missing_count": len(missing),
                "missing_brands": missing[:5],  # Limit to 5
            })

        return {
            "table": table_name,
            "marketplace": marketplace,
            "expected_brands": len(expected_brands),
            "days_checked": len(date_results),
            "date_results": date_results,
        }
    except Exception as e:
        logger.error(f"Error validating BA Dash dates: {e}")
        return {"error": str(e)}


async def run_full_validation(table_name: str, fix_duplicates: bool = False) -> Dict:
    """
    Run full validation on a table: duplicates, header rows, and brand validation.
    Optionally fix issues found.

    Args:
        table_name: Name of the table
        fix_duplicates: If True, automatically remove duplicates and header rows

    Returns:
        Dict with full validation results
    """
    results = {
        "table": table_name,
        "timestamp": datetime.utcnow().isoformat(),
    }

    # 1. Check for duplicates
    dup_result = detect_duplicates(table_name)
    results["duplicates"] = dup_result

    # 2. Check for header rows
    header_result = remove_header_rows(table_name, dry_run=True)
    results["header_rows"] = header_result

    # 3. Validate brands
    brand_result = validate_brands(table_name)
    results["brand_validation"] = brand_result

    # 4. For BA Dash tables, also check date-based validation
    config = TABLE_CONFIG.get(table_name, {})
    if config.get("date_column"):
        date_result = validate_ba_dash_dates(table_name)
        results["date_validation"] = date_result

    # 5. Fix issues if requested
    if fix_duplicates:
        if dup_result.get("extra_rows", 0) > 0:
            fix_dup = remove_duplicates(table_name, dry_run=False)
            results["fix_duplicates"] = fix_dup

        if header_result.get("header_rows_found", 0) > 0:
            fix_header = remove_header_rows(table_name, dry_run=False)
            results["fix_headers"] = fix_header

    # 6. Send Slack notification
    await send_validation_report(results)

    return results


async def send_validation_report(results: Dict) -> None:
    """Send validation results to Slack."""
    table = results.get("table", "unknown")

    # Build message
    lines = [f"📊 *iBot v2 Validation - {table}*\n"]

    # Duplicates
    dup = results.get("duplicates", {})
    if "error" not in dup:
        extra = dup.get("extra_rows", 0)
        if extra > 0:
            lines.append(f"🔴 *Duplicates:* {extra} extra rows ({dup.get('duplicate_combinations', 0)} combinations)")
        else:
            lines.append("✅ *Duplicates:* None found")

    # Header rows
    hdr = results.get("header_rows", {})
    if hdr.get("header_rows_found", 0) > 0:
        lines.append(f"⚠️ *Header rows:* {hdr['header_rows_found']} found")

    # Brand validation
    brand = results.get("brand_validation", {})
    if "error" not in brand:
        missing = brand.get("missing_count", 0)
        if missing > 0:
            missing_list = ", ".join(brand.get("missing_brands", [])[:5])
            extra = f" +{missing - 5} more" if missing > 5 else ""
            lines.append(f"⚠️ *Missing brands:* {missing} - {missing_list}{extra}")
        else:
            lines.append(f"✅ *Brands:* All {brand.get('found_count', 0)} found")

    # Date validation (for BA Dash)
    date_val = results.get("date_validation", {})
    if date_val and "error" not in date_val:
        days_with_missing = sum(1 for d in date_val.get("date_results", []) if d.get("missing_count", 0) > 0)
        total_days = len(date_val.get("date_results", []))
        if days_with_missing > 0:
            lines.append(f"⚠️ *L7D:* {days_with_missing}/{total_days} days with missing brands")
        else:
            lines.append(f"✅ *L7D:* All brands found for {total_days} days")

    # Fixes applied
    if results.get("fix_duplicates"):
        fix = results["fix_duplicates"]
        lines.append(f"\n🔧 *Fixed:* Removed {fix.get('rows_removed', 0)} duplicate rows")

    if results.get("fix_headers"):
        fix = results["fix_headers"]
        lines.append(f"🔧 *Fixed:* Removed {fix.get('rows_removed', 0)} header rows")

    message = "\n".join(lines)

    try:
        notifier = get_notifier()
        await notifier.send_message(message)
    except Exception as e:
        logger.error(f"Failed to send Slack notification: {e}")


def validate_all_tables(fix_issues: bool = False) -> Dict:
    """
    Validate all configured tables.

    Args:
        fix_issues: If True, automatically fix duplicates and header rows

    Returns:
        Dict with results for all tables
    """
    import asyncio

    results = {}
    for table_name in TABLE_CONFIG.keys():
        try:
            result = asyncio.get_event_loop().run_until_complete(
                run_full_validation(table_name, fix_duplicates=fix_issues)
            )
            results[table_name] = result
        except Exception as e:
            logger.error(f"Error validating {table_name}: {e}")
            results[table_name] = {"error": str(e)}

    return results


# CLI interface for manual validation
if __name__ == "__main__":
    import sys
    import json

    if len(sys.argv) < 2:
        print("Usage: python validation.py <table_name> [--fix]")
        print("       python validation.py --all [--fix]")
        print("\nAvailable tables:")
        for t in TABLE_CONFIG.keys():
            print(f"  - {t}")
        sys.exit(1)

    table = sys.argv[1]
    fix = "--fix" in sys.argv

    if table == "--all":
        results = validate_all_tables(fix_issues=fix)
        print(json.dumps(results, indent=2, default=str))
    else:
        if table not in TABLE_CONFIG:
            print(f"Unknown table: {table}")
            sys.exit(1)

        import asyncio
        result = asyncio.get_event_loop().run_until_complete(
            run_full_validation(table, fix_duplicates=fix)
        )
        print(json.dumps(result, indent=2, default=str))
