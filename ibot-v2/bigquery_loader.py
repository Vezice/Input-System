"""
iBot v2 BigQuery Loader

Handles uploading parsed data to BigQuery.
Uses streaming inserts for real-time data loading.
"""

import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from config import CategoryConfig, settings
from parser import ParsedFile
from utils.logger import get_logger

logger = get_logger(__name__)


def sanitize_column_name(name: str) -> str:
    """
    Sanitize a column name for BigQuery compatibility.

    BigQuery column name rules:
    - Must contain only letters, numbers, and underscores
    - Must start with a letter or underscore
    - Max 300 characters
    """
    if not name:
        return "_empty_"

    # Replace common problematic characters
    replacements = {
        '/': '_', '\\': '_', ' ': '_', '-': '_', '.': '_',
        '(': '_', ')': '_', '[': '_', ']': '_', '{': '_', '}': '_',
        '%': '_pct', '#': '_num', '@': '_at', '&': '_and',
        '+': '_plus', '=': '_eq', '<': '_lt', '>': '_gt',
        '!': '_', '?': '_', ':': '_', ';': '_', ',': '_',
        "'": '_', '"': '_',
    }

    sanitized = name
    for char, replacement in replacements.items():
        sanitized = sanitized.replace(char, replacement)

    # Remove any remaining invalid characters
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', sanitized)

    # Collapse multiple underscores
    sanitized = re.sub(r'_+', '_', sanitized)

    # Remove leading/trailing underscores
    sanitized = sanitized.strip('_')

    # Ensure starts with letter or underscore
    if sanitized and sanitized[0].isdigit():
        sanitized = '_' + sanitized

    # Handle empty result
    if not sanitized:
        sanitized = '_column_'

    # Truncate to 300 characters max
    if len(sanitized) > 300:
        sanitized = sanitized[:300]

    return sanitized


class BigQueryLoader:
    """Handles BigQuery operations for iBot imports."""

    def __init__(
        self,
        project_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
    ):
        self.project_id = project_id or settings.PROJECT_ID
        self.dataset_id = dataset_id or settings.BIGQUERY_DATASET
        self._client: Optional[bigquery.Client] = None

    @property
    def client(self) -> bigquery.Client:
        """Lazy-load BigQuery client."""
        if self._client is None:
            self._client = bigquery.Client(project=self.project_id)
        return self._client

    def get_table_id(self, table_name: str) -> str:
        """Get fully qualified table ID."""
        return f"{self.project_id}.{self.dataset_id}.{table_name}"

    def ensure_dataset_exists(self) -> None:
        """Create dataset if it doesn't exist."""
        dataset_ref = bigquery.DatasetReference(self.project_id, self.dataset_id)

        try:
            self.client.get_dataset(dataset_ref)
            logger.debug(f"Dataset {self.dataset_id} already exists")
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "asia-southeast2"  # Jakarta region
            dataset.description = "iBot v2 data imports"
            self.client.create_dataset(dataset)
            logger.info(f"Created dataset {self.dataset_id}")

    def get_schema(self, parsed_file: ParsedFile) -> List[bigquery.SchemaField]:
        """
        Build schema from parsed file headers.

        All data columns are STRING type (type casting done in downstream dbt/queries).
        """
        # Metadata columns (only brand code)
        schema = [
            bigquery.SchemaField("_brand_code", "STRING", mode="REQUIRED"),
        ]

        # Data columns - all STRING
        used_names = {f.name.lower() for f in schema}
        for header in parsed_file.headers:
            sanitized_name = sanitize_column_name(header)

            # Handle duplicates
            if sanitized_name.lower() in used_names:
                counter = 2
                while f"{sanitized_name}_{counter}".lower() in used_names:
                    counter += 1
                sanitized_name = f"{sanitized_name}_{counter}"

            schema.append(
                bigquery.SchemaField(
                    name=sanitized_name,
                    field_type="STRING",
                    mode="NULLABLE",
                )
            )
            used_names.add(sanitized_name.lower())

        return schema

    def ensure_table_exists(
        self,
        table_name: str,
        schema: List[bigquery.SchemaField],
    ) -> bigquery.Table:
        """Create table if it doesn't exist, or update schema if needed."""
        table_id = self.get_table_id(table_name)

        try:
            table = self.client.get_table(table_id)
            logger.debug(f"Table {table_name} already exists")

            # Check if schema needs updating
            existing_fields = {f.name.lower() for f in table.schema}
            new_fields = [f for f in schema if f.name.lower() not in existing_fields]

            if new_fields:
                # Add new fields (BigQuery allows adding nullable columns)
                updated_schema = list(table.schema) + new_fields
                table.schema = updated_schema
                self.client.update_table(table, ["schema"])
                logger.info(f"Updated schema for {table_name}: added {len(new_fields)} fields")

            return table

        except NotFound:
            # Create new table
            table = bigquery.Table(table_id, schema=schema)

            # Add partitioning
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="_import_timestamp",
            )

            # Add clustering
            table.clustering_fields = ["_brand_code", "_source_file"]

            table = self.client.create_table(table)
            logger.info(f"Created table {table_name} with {len(schema)} columns")

            # Wait for table to be ready
            import time
            time.sleep(5)

            return table

    def prepare_rows(
        self,
        parsed_file: ParsedFile,
        brand_code: str,
        import_id: str,
    ) -> List[Dict[str, Any]]:
        """
        Prepare rows for BigQuery insert by adding metadata.

        All values converted to strings.
        """
        prepared_rows = []

        # Build column name mapping (original -> sanitized)
        column_mapping = {}
        used_names = set()
        for header in parsed_file.headers:
            sanitized = sanitize_column_name(header)
            if sanitized in used_names:
                counter = 2
                while f"{sanitized}_{counter}" in used_names:
                    counter += 1
                sanitized = f"{sanitized}_{counter}"
            column_mapping[header] = sanitized
            used_names.add(sanitized)

        for row in parsed_file.rows:
            # Process each cell value with sanitized column names
            cleaned_row = {}
            for original_key, value in row.items():
                sanitized_key = column_mapping.get(original_key, sanitize_column_name(original_key))
                # Convert value to string, handle None
                if value is None:
                    cleaned_row[sanitized_key] = ""
                else:
                    cleaned_row[sanitized_key] = str(value)

            prepared_row = {
                "_brand_code": brand_code,
                **cleaned_row,
            }
            prepared_rows.append(prepared_row)

        return prepared_rows

    def streaming_insert(
        self,
        table_name: str,
        rows: List[Dict[str, Any]],
        batch_size: int = 500,
    ) -> Tuple[int, List[str]]:
        """
        Insert rows using streaming API.
        """
        table_id = self.get_table_id(table_name)
        table = self.client.get_table(table_id)

        total_inserted = 0
        errors = []

        # Process in batches
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]

            insert_errors = self.client.insert_rows_json(table, batch)

            if insert_errors:
                for error in insert_errors:
                    error_msg = f"Row {error['index']}: {error['errors']}"
                    errors.append(error_msg)
                    logger.warning(f"Insert error: {error_msg}")
            else:
                total_inserted += len(batch)

            logger.debug(f"Inserted batch {i // batch_size + 1}: {len(batch)} rows")

        logger.info(
            f"Streaming insert complete: {total_inserted}/{len(rows)} rows",
            table=table_name,
            errors=len(errors),
        )

        return total_inserted, errors

    def delete_brand_data(self, brand_code: str, table_name: str) -> int:
        """
        Delete all rows for a specific brand from a table.

        Used for BA Produk categories which are daily snapshots.
        """
        table_id = self.get_table_id(table_name)

        query = f"""
        DELETE FROM `{table_id}`
        WHERE _brand_code = @brand_code
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("brand_code", "STRING", brand_code),
            ]
        )

        try:
            query_job = self.client.query(query, job_config=job_config)
            query_job.result()

            rows_deleted = query_job.num_dml_affected_rows
            logger.info(
                f"Deleted {rows_deleted} rows for brand {brand_code} from {table_name}",
                brand_code=brand_code,
                table=table_name,
            )
            return rows_deleted
        except NotFound:
            logger.debug(f"Table {table_name} not found, nothing to delete")
            return 0
        except Exception as e:
            logger.warning(f"Failed to delete brand data: {e}")
            return 0

    def upload(
        self,
        parsed_file: ParsedFile,
        brand_code: str,
        import_id: str,
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Upload a parsed file to BigQuery.

        Returns:
            Tuple of (success, result_info)
        """
        if not parsed_file.is_valid:
            return False, {"error": "Invalid file - no data"}

        category = parsed_file.category

        logger.info(
            f"Uploading to BigQuery",
            import_id=import_id,
            table=category.bigquery_table,
            rows=len(parsed_file.rows),
        )

        try:
            # Ensure dataset exists
            self.ensure_dataset_exists()

            # Get schema
            schema = self.get_schema(parsed_file)

            # Ensure table exists
            self.ensure_table_exists(category.bigquery_table, schema)

            # BA Produk categories are daily snapshots - DELETE existing brand data first
            if "BA Produk" in category.name:
                deleted = self.delete_brand_data(brand_code, category.bigquery_table)
                logger.info(f"Deleted {deleted} existing rows for {brand_code} before import")

            # Prepare rows
            prepared_rows = self.prepare_rows(parsed_file, brand_code, import_id)

            # Upload using streaming insert
            rows_inserted, errors = self.streaming_insert(
                category.bigquery_table,
                prepared_rows,
                batch_size=settings.BATCH_SIZE,
            )

            return len(errors) == 0, {
                "import_id": import_id,
                "table": category.bigquery_table,
                "rows_inserted": rows_inserted,
                "rows_total": len(prepared_rows),
                "errors": errors,
            }

        except Exception as e:
            logger.error(f"BigQuery upload failed: {e}", exc_info=True)
            return False, {
                "import_id": import_id,
                "error": str(e),
            }


# Module-level convenience instance
_loader: Optional[BigQueryLoader] = None


def get_loader() -> BigQueryLoader:
    """Get or create the BigQuery loader instance."""
    global _loader
    if _loader is None:
        _loader = BigQueryLoader()
    return _loader
