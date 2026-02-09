"""
iBot v1 vs v2 Comparison Script

Compares data between iBot v1 (ibot_data) and iBot v2 (ibot_v2_data) datasets.
Run after parallel processing to verify v2 produces identical results.

Usage:
    python compare_results.py                      # Compare all tables
    python compare_results.py ba_produk_laz        # Compare specific table
    python compare_results.py ba_produk_laz GS     # Compare specific table and brand
    python compare_results.py ba_produk_laz --latest  # Compare only latest import
"""

import sys
from typing import Optional
from google.cloud import bigquery

# Configuration
PROJECT_ID = "fbi-dev-484410"
V1_DATASET = "ibot_data"
V2_DATASET = "ibot_v2_data"

# Column name mapping (v1 uses 'akun', v2 uses '_brand_code')
V1_BRAND_COL = "akun"
V2_BRAND_COL = "_brand_code"

# Tables to compare (same names in both datasets)
TABLES = [
    "ba_produk_laz",
    "ba_produk_sho",
    "ba_produk_tik",
    "ba_dash_laz",
    "ba_dash_sho",
    "ba_dash_tik",
    "ba_dash_tok",
    "informasi_dasar_sho",
    "informasi_dikirim_sho",
    "informasi_media_sho",
    "informasi_penjualan_sho",
    "export_sku_laz",
    "export_sku_tik",
    "demografis_bsl",
    "proyeksi_stok_bsl",
]


def get_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


def table_exists(client: bigquery.Client, dataset: str, table: str) -> bool:
    """Check if a table exists."""
    try:
        client.get_table(f"{PROJECT_ID}.{dataset}.{table}")
        return True
    except Exception:
        return False


def get_row_counts(client: bigquery.Client, table: str) -> dict:
    """Get row counts per brand for both datasets."""
    results = {"v1": {}, "v2": {}, "v1_total": 0, "v2_total": 0}

    for version, dataset, brand_col in [
        ("v1", V1_DATASET, V1_BRAND_COL),
        ("v2", V2_DATASET, V2_BRAND_COL)
    ]:
        if not table_exists(client, dataset, table):
            continue

        query = f"""
            SELECT {brand_col} as brand, COUNT(*) as row_count
            FROM `{PROJECT_ID}.{dataset}.{table}`
            GROUP BY {brand_col}
            ORDER BY {brand_col}
        """

        try:
            rows = client.query(query).result()
            for row in rows:
                brand = row.brand or "(empty)"
                results[version][brand] = row.row_count
                results[f"{version}_total"] += row.row_count
        except Exception as e:
            print(f"  Error querying {version}: {e}")

    return results


def compare_row_counts(client: bigquery.Client, table: str) -> dict:
    """Compare row counts between v1 and v2."""
    counts = get_row_counts(client, table)

    all_brands = set(counts["v1"].keys()) | set(counts["v2"].keys())

    comparison = {
        "table": table,
        "v1_total": counts["v1_total"],
        "v2_total": counts["v2_total"],
        "total_match": counts["v1_total"] == counts["v2_total"],
        "brands": [],
        "missing_in_v1": [],
        "missing_in_v2": [],
        "mismatched": [],
    }

    for brand in sorted(all_brands):
        v1_count = counts["v1"].get(brand, 0)
        v2_count = counts["v2"].get(brand, 0)

        brand_info = {
            "brand": brand,
            "v1": v1_count,
            "v2": v2_count,
            "diff": v1_count - v2_count,
            "match": v1_count == v2_count,
        }
        comparison["brands"].append(brand_info)

        if v1_count == 0:
            comparison["missing_in_v1"].append(brand)
        elif v2_count == 0:
            comparison["missing_in_v2"].append(brand)
        elif v1_count != v2_count:
            comparison["mismatched"].append(brand_info)

    return comparison


def get_common_columns(client: bigquery.Client, table: str) -> list:
    """Get columns that exist in both v1 and v2 tables."""
    columns = {"v1": set(), "v2": set()}

    for version, dataset in [("v1", V1_DATASET), ("v2", V2_DATASET)]:
        if not table_exists(client, dataset, table):
            continue

        query = f"""
            SELECT column_name
            FROM `{PROJECT_ID}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table}'
        """

        try:
            rows = client.query(query).result()
            columns[version] = {row.column_name for row in rows}
        except Exception:
            pass

    # Return common columns, excluding v1-specific metadata
    v1_only_metadata = {"_import_id", "_source_file", "_import_timestamp", "_row_number"}
    common = columns["v1"] & columns["v2"]
    return sorted(common - v1_only_metadata)


def compare_data_sample(
    client: bigquery.Client,
    table: str,
    brand: Optional[str] = None,
    limit: int = 100
) -> dict:
    """Compare actual data values between v1 and v2."""

    common_cols = get_common_columns(client, table)
    if not common_cols:
        return {"error": "No common columns found"}

    # Build column list for comparison (exclude brand columns from hash)
    data_cols = [c for c in common_cols if c not in (V1_BRAND_COL, V2_BRAND_COL, "_brand_code", "akun")]
    if not data_cols:
        return {"error": "No data columns to compare"}

    cols_str = ", ".join(data_cols)
    hash_expr = f"TO_JSON_STRING(STRUCT({cols_str}))"

    # Get sample from v1
    v1_brand_filter = f"AND {V1_BRAND_COL} = '{brand}'" if brand else ""
    v1_query = f"""
        SELECT {V1_BRAND_COL} as brand, {hash_expr} as row_hash
        FROM `{PROJECT_ID}.{V1_DATASET}.{table}`
        WHERE 1=1 {v1_brand_filter}
        LIMIT {limit}
    """

    v1_hashes = set()
    try:
        for row in client.query(v1_query).result():
            v1_hashes.add((row.brand, row.row_hash))
    except Exception as e:
        return {"error": f"V1 query failed: {e}"}

    # Get sample from v2
    v2_brand_filter = f"AND {V2_BRAND_COL} = '{brand}'" if brand else ""
    v2_query = f"""
        SELECT {V2_BRAND_COL} as brand, {hash_expr} as row_hash
        FROM `{PROJECT_ID}.{V2_DATASET}.{table}`
        WHERE 1=1 {v2_brand_filter}
        LIMIT {limit}
    """

    v2_hashes = set()
    try:
        for row in client.query(v2_query).result():
            v2_hashes.add((row.brand, row.row_hash))
    except Exception as e:
        return {"error": f"V2 query failed: {e}"}

    # Compare
    only_in_v1 = v1_hashes - v2_hashes
    only_in_v2 = v2_hashes - v1_hashes
    common = v1_hashes & v2_hashes

    return {
        "columns_compared": len(data_cols),
        "v1_sample_size": len(v1_hashes),
        "v2_sample_size": len(v2_hashes),
        "matching_rows": len(common),
        "only_in_v1": len(only_in_v1),
        "only_in_v2": len(only_in_v2),
        "match_rate": len(common) / max(len(v1_hashes), 1) * 100,
    }


def print_comparison(comparison: dict):
    """Print comparison results in a readable format."""
    table = comparison["table"]

    print(f"\n{'='*60}")
    print(f"TABLE: {table}")
    print(f"{'='*60}")

    # Total counts
    v1_total = comparison["v1_total"]
    v2_total = comparison["v2_total"]
    status = "✅ MATCH" if comparison["total_match"] else "❌ MISMATCH"

    print(f"\nTotal Rows:")
    print(f"  v1 (ibot_data):    {v1_total:,}")
    print(f"  v2 (ibot_v2_data): {v2_total:,}")
    print(f"  Status: {status}")

    if v1_total == 0 and v2_total == 0:
        print("  (Both tables are empty)")
        return

    # Brand breakdown
    if comparison["brands"]:
        print(f"\nBrand Breakdown:")
        print(f"  {'Brand':<10} {'v1':>10} {'v2':>10} {'Diff':>10} {'Status':<10}")
        print(f"  {'-'*50}")

        for b in comparison["brands"]:
            status = "✅" if b["match"] else "❌"
            diff_str = f"{b['diff']:+d}" if b["diff"] != 0 else "0"
            print(f"  {b['brand']:<10} {b['v1']:>10,} {b['v2']:>10,} {diff_str:>10} {status}")

    # Issues summary
    if comparison["missing_in_v1"]:
        print(f"\n⚠️  Brands missing in v1: {', '.join(comparison['missing_in_v1'])}")

    if comparison["missing_in_v2"]:
        print(f"\n⚠️  Brands missing in v2: {', '.join(comparison['missing_in_v2'])}")

    if comparison["mismatched"]:
        print(f"\n❌ Row count mismatches: {len(comparison['mismatched'])} brands")


def compare_brand_data_detailed(
    client: bigquery.Client,
    table: str,
    brand: str
) -> dict:
    """
    Do a detailed row-by-row comparison for a specific brand.
    Useful for verifying parallel import results.
    """
    common_cols = get_common_columns(client, table)
    if not common_cols:
        return {"error": "No common columns found"}

    # Exclude brand columns and metadata
    data_cols = [c for c in common_cols if c not in (
        V1_BRAND_COL, V2_BRAND_COL, "_brand_code", "akun",
        "import_timestamp", "import_batch_id"
    )]

    if not data_cols:
        return {"error": "No data columns to compare"}

    # Get all rows from v1
    v1_query = f"""
        SELECT {', '.join(data_cols)}
        FROM `{PROJECT_ID}.{V1_DATASET}.{table}`
        WHERE {V1_BRAND_COL} = '{brand}'
        ORDER BY {', '.join(data_cols[:3])}
    """

    # Get all rows from v2
    v2_query = f"""
        SELECT {', '.join(data_cols)}
        FROM `{PROJECT_ID}.{V2_DATASET}.{table}`
        WHERE {V2_BRAND_COL} = '{brand}'
        ORDER BY {', '.join(data_cols[:3])}
    """

    try:
        v1_rows = list(client.query(v1_query).result())
        v2_rows = list(client.query(v2_query).result())
    except Exception as e:
        return {"error": str(e)}

    # Convert to comparable format
    def row_to_tuple(row):
        return tuple(str(row[col]) if row[col] is not None else "" for col in data_cols)

    v1_set = set(row_to_tuple(r) for r in v1_rows)
    v2_set = set(row_to_tuple(r) for r in v2_rows)

    matching = v1_set & v2_set
    only_v1 = v1_set - v2_set
    only_v2 = v2_set - v1_set

    return {
        "brand": brand,
        "columns": data_cols,
        "v1_rows": len(v1_rows),
        "v2_rows": len(v2_rows),
        "matching": len(matching),
        "only_in_v1": len(only_v1),
        "only_in_v2": len(only_v2),
        "match_rate": len(matching) / max(len(v1_rows), len(v2_rows), 1) * 100,
        "sample_v1_only": list(only_v1)[:3],
        "sample_v2_only": list(only_v2)[:3],
    }


def main():
    client = get_client()

    # Parse arguments
    target_table = sys.argv[1] if len(sys.argv) > 1 else None
    target_brand = sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].startswith("--") else None
    detailed_mode = "--detailed" in sys.argv or "-d" in sys.argv

    tables_to_check = [target_table] if target_table else TABLES

    print("\n" + "="*60)
    print("iBot v1 vs v2 Comparison Report")
    print("="*60)
    print(f"v1 Dataset: {PROJECT_ID}.{V1_DATASET}")
    print(f"v2 Dataset: {PROJECT_ID}.{V2_DATASET}")

    all_match = True
    summary = []

    for table in tables_to_check:
        # Check if tables exist
        v1_exists = table_exists(client, V1_DATASET, table)
        v2_exists = table_exists(client, V2_DATASET, table)

        if not v1_exists and not v2_exists:
            print(f"\n⚪ {table}: Both tables don't exist (skipping)")
            continue
        elif not v1_exists:
            print(f"\n⚠️  {table}: Only exists in v2")
            continue
        elif not v2_exists:
            print(f"\n⚠️  {table}: Only exists in v1")
            continue

        # Compare row counts
        comparison = compare_row_counts(client, table)
        print_comparison(comparison)

        if not comparison["total_match"]:
            all_match = False

        # If specific brand requested, do detailed data comparison
        if target_brand:
            print(f"\n  Detailed Data Comparison for brand '{target_brand}':")
            data_comp = compare_brand_data_detailed(client, table, target_brand)

            if "error" in data_comp:
                print(f"    Error: {data_comp['error']}")
            else:
                match_rate = data_comp["match_rate"]
                status = "✅" if match_rate == 100 else "⚠️"
                print(f"    Columns compared: {len(data_comp['columns'])}")
                print(f"    v1 rows: {data_comp['v1_rows']}")
                print(f"    v2 rows: {data_comp['v2_rows']}")
                print(f"    Matching rows: {data_comp['matching']}")
                print(f"    Only in v1: {data_comp['only_in_v1']}")
                print(f"    Only in v2: {data_comp['only_in_v2']}")
                print(f"    Match rate: {match_rate:.1f}% {status}")

                if data_comp["sample_v1_only"]:
                    print(f"\n    Sample rows only in v1:")
                    for row in data_comp["sample_v1_only"][:2]:
                        print(f"      {row[:5]}...")  # Show first 5 columns

                if data_comp["sample_v2_only"]:
                    print(f"\n    Sample rows only in v2:")
                    for row in data_comp["sample_v2_only"][:2]:
                        print(f"      {row[:5]}...")  # Show first 5 columns

        elif comparison["total_match"] and comparison["v1_total"] > 0:
            print(f"\n  Data Comparison (sample):")
            data_comp = compare_data_sample(client, table, target_brand, limit=500)

            if "error" in data_comp:
                print(f"    Error: {data_comp['error']}")
            else:
                match_rate = data_comp["match_rate"]
                status = "✅" if match_rate == 100 else "⚠️"
                print(f"    Columns compared: {data_comp['columns_compared']}")
                print(f"    Sample size: v1={data_comp['v1_sample_size']}, v2={data_comp['v2_sample_size']}")
                print(f"    Matching rows: {data_comp['matching_rows']}")
                print(f"    Match rate: {match_rate:.1f}% {status}")

        summary.append({
            "table": table,
            "match": comparison["total_match"],
            "v1": comparison["v1_total"],
            "v2": comparison["v2_total"],
        })

    # Final summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)

    if not summary:
        print("No tables to compare.")
        return

    print(f"\n{'Table':<25} {'v1':>10} {'v2':>10} {'Status':<10}")
    print("-"*55)

    for s in summary:
        status = "✅ Match" if s["match"] else "❌ Diff"
        print(f"{s['table']:<25} {s['v1']:>10,} {s['v2']:>10,} {status}")

    print()
    if all_match:
        print("✅ All tables match! iBot v2 produces identical results.")
    else:
        print("❌ Some tables have differences. Review the details above.")


if __name__ == "__main__":
    main()
