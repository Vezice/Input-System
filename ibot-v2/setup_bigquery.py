#!/usr/bin/env python3
"""
iBot v2 BigQuery Setup Script

Creates the dataset and base tables for all categories.
Tables are created with metadata columns only - data columns are added
dynamically when files are uploaded.

Usage:
    python setup_bigquery.py
    python setup_bigquery.py --dry-run
"""

import argparse
import sys
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Configuration
PROJECT_ID = "fbi-dev-484410"
DATASET_ID = "ibot_v2_data"
REGION = "asia-southeast2"  # Jakarta

# Categories from Admin Sheet Type Validation
# Table names are category names converted to lowercase with underscores
CATEGORIES = [
    # BA Dash - Dashboard data
    "BA Dash LAZ",
    "BA Dash SHO",
    "BA Dash TIK",
    "BA Dash TOK",
    # BA Produk - Product data (snapshot mode - deletes before insert)
    "BA Produk LAZ",
    "BA Produk SHO",
    "BA Produk TIK",
    # Shopee specific data
    "Informasi Dasar SHO",
    "Informasi Dikirim SHO",
    "Informasi Media SHO",
    "Informasi Penjualan SHO",
    # Export SKU
    "Export SKU LAZ",
    "Export SKU TIK",
    # BSL specific
    "Demografis BSL",
    "Proyeksi Stok BSL",
]


def category_to_table_name(category: str) -> str:
    """Convert category name to BigQuery table name."""
    return category.lower().replace(" ", "_")


def get_base_schema() -> list:
    """Get the base schema with metadata columns only."""
    return [
        bigquery.SchemaField("_import_id", "STRING", mode="REQUIRED",
                            description="Unique import identifier"),
        bigquery.SchemaField("_brand_code", "STRING", mode="REQUIRED",
                            description="Brand code extracted from filename"),
        bigquery.SchemaField("_source_file", "STRING", mode="REQUIRED",
                            description="Original source filename"),
        bigquery.SchemaField("_import_timestamp", "TIMESTAMP", mode="REQUIRED",
                            description="When the data was imported"),
        bigquery.SchemaField("_row_number", "INTEGER", mode="REQUIRED",
                            description="Row number in source file"),
    ]


def create_dataset(client: bigquery.Client, dry_run: bool = False) -> bool:
    """Create the dataset if it doesn't exist."""
    dataset_ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID)

    try:
        client.get_dataset(dataset_ref)
        print(f"[OK] Dataset {DATASET_ID} already exists")
        return True
    except NotFound:
        if dry_run:
            print(f"[DRY-RUN] Would create dataset {DATASET_ID}")
            return True

        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = REGION
        dataset.description = "iBot v2 data imports - Excel/CSV files processed from GCS"

        client.create_dataset(dataset)
        print(f"[CREATED] Dataset {DATASET_ID}")
        return True


def create_table(
    client: bigquery.Client,
    category: str,
    dry_run: bool = False
) -> bool:
    """Create a table for a category if it doesn't exist."""
    table_name = category_to_table_name(category)
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    try:
        table = client.get_table(table_id)
        print(f"[OK] Table {table_name} already exists ({len(table.schema)} columns)")
        return True
    except NotFound:
        if dry_run:
            print(f"[DRY-RUN] Would create table {table_name}")
            return True

        schema = get_base_schema()
        table = bigquery.Table(table_id, schema=schema)

        # Add partitioning by import timestamp
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="_import_timestamp",
        )

        # Add clustering for faster queries
        table.clustering_fields = ["_brand_code", "_source_file"]

        # Add description
        table.description = f"iBot v2 imports for {category}"

        client.create_table(table)
        print(f"[CREATED] Table {table_name}")
        return True


def main():
    parser = argparse.ArgumentParser(
        description="Set up BigQuery dataset and tables for iBot v2"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be created without making changes"
    )
    parser.add_argument(
        "--project",
        default=PROJECT_ID,
        help=f"GCP project ID (default: {PROJECT_ID})"
    )
    args = parser.parse_args()

    print("=" * 60)
    print("iBot v2 BigQuery Setup")
    print("=" * 60)
    print(f"Project: {args.project}")
    print(f"Dataset: {DATASET_ID}")
    print(f"Region: {REGION}")
    print(f"Categories: {len(CATEGORIES)}")
    if args.dry_run:
        print("\n*** DRY RUN MODE - No changes will be made ***\n")
    print("=" * 60)
    print()

    # Initialize client
    try:
        client = bigquery.Client(project=args.project)
    except Exception as e:
        print(f"[ERROR] Failed to initialize BigQuery client: {e}")
        print("\nMake sure you have:")
        print("  1. Installed google-cloud-bigquery: pip install google-cloud-bigquery")
        print("  2. Authenticated: gcloud auth application-default login")
        sys.exit(1)

    # Create dataset
    print("Checking dataset...")
    if not create_dataset(client, args.dry_run):
        sys.exit(1)
    print()

    # Create tables
    print("Checking tables...")
    success_count = 0
    for category in CATEGORIES:
        if create_table(client, category, args.dry_run):
            success_count += 1

    print()
    print("=" * 60)
    print(f"Setup complete: {success_count}/{len(CATEGORIES)} tables ready")
    print("=" * 60)
    print()
    print("Tables are created with metadata columns only.")
    print("Data columns will be added automatically when files are uploaded.")
    print()
    print("GCS bucket for uploads:")
    print(f"  gs://ibot-v2-imports/{{CATEGORY}}/{{BRAND}} filename.xlsx")
    print()
    print("Example:")
    print(f"  gsutil cp data.xlsx gs://ibot-v2-imports/BA\\ Produk\\ LAZ/GS\\ data.xlsx")


if __name__ == "__main__":
    main()
