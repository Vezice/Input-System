"""
iBot v2 BigQuery Loader

Handles uploading parsed data to BigQuery.
Uses streaming inserts for real-time data loading.

Column naming: Excel-style (A, B, C, ... Z, AA, AB, etc.)
Row 1 contains original headers for reference.
"""

import random
import time
from typing import Any, Dict, List, Optional, Tuple

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import BadRequest, Conflict

from config import CategoryConfig, settings
from parser import ParsedFile
from utils.logger import get_logger

logger = get_logger(__name__)


def get_column_name(index: int) -> str:
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
            dataset.description = "iBot v2 data imports (Bronze layer)"
            self.client.create_dataset(dataset)
            logger.info(f"Created dataset {self.dataset_id}")

    def get_schema(self, num_columns: int) -> List[bigquery.SchemaField]:
        """
        Build schema with Excel-style column names.

        Schema: _brand_code, A, B, C, ... (all STRING)
        """
        # Metadata column
        schema = [
            bigquery.SchemaField("_brand_code", "STRING", mode="REQUIRED"),
        ]

        # Data columns: A, B, C, ... (Excel-style)
        for i in range(num_columns):
            schema.append(
                bigquery.SchemaField(
                    name=get_column_name(i),
                    field_type="STRING",
                    mode="NULLABLE",
                )
            )

        return schema

    def ensure_table_exists(
        self,
        table_name: str,
        schema: List[bigquery.SchemaField],
        max_retries: int = 5,
    ) -> bigquery.Table:
        """Create table if it doesn't exist, or update schema if needed.

        Includes retry logic to handle race conditions when multiple functions
        try to create the same table simultaneously.
        """
        table_id = self.get_table_id(table_name)

        for attempt in range(max_retries):
            try:
                table = self.client.get_table(table_id)
                logger.debug(f"Table {table_name} already exists")

                # Check if schema needs updating (more columns needed)
                existing_fields = {f.name for f in table.schema}
                new_fields = [f for f in schema if f.name not in existing_fields]

                if new_fields:
                    # Add new fields (BigQuery allows adding nullable columns)
                    updated_schema = list(table.schema) + new_fields
                    table.schema = updated_schema
                    self.client.update_table(table, ["schema"])
                    logger.info(f"Updated schema for {table_name}: added {len(new_fields)} columns")

                return table

            except NotFound:
                # Create new table
                try:
                    table = bigquery.Table(table_id, schema=schema)

                    # Add clustering by brand code for efficient queries
                    table.clustering_fields = ["_brand_code"]

                    table = self.client.create_table(table)
                    logger.info(f"Created table {table_name} with {len(schema)} columns")

                    # Wait for table to be fully propagated
                    # This is critical for preventing race conditions
                    time.sleep(8)

                    return table

                except Conflict:
                    # Table was created by another concurrent function
                    # Wait with exponential backoff and retry
                    wait_time = (2 ** attempt) + random.random()
                    logger.info(
                        f"Table {table_name} created by another process, "
                        f"waiting {wait_time:.1f}s before retry..."
                    )
                    time.sleep(wait_time)
                    continue

        # All retries exhausted, try one more get with a final wait
        time.sleep(3)
        return self.client.get_table(table_id)

    def prepare_rows(
        self,
        parsed_file: ParsedFile,
        brand_code: str,
        import_id: str,
    ) -> Tuple[Dict[str, str], List[Dict[str, Any]]]:
        """
        Prepare rows for BigQuery insert.

        Returns:
            Tuple of (header_row, data_rows)
            - header_row: Row with original column headers (brand_code = "_header_")
            - data_rows: Actual data rows with Excel-style column names
        """
        # Build header row (Row 1 in BigQuery)
        header_row = {"_brand_code": "_header_"}
        for i, header in enumerate(parsed_file.headers):
            col_name = get_column_name(i)
            header_row[col_name] = str(header) if header else ""

        # Build data rows
        data_rows = []
        for row in parsed_file.rows:
            prepared_row = {"_brand_code": brand_code}

            for i, header in enumerate(parsed_file.headers):
                col_name = get_column_name(i)
                value = row.get(header, "")
                # Convert value to string, handle None
                if value is None:
                    prepared_row[col_name] = ""
                else:
                    prepared_row[col_name] = str(value)

            data_rows.append(prepared_row)

        return header_row, data_rows

    def streaming_insert(
        self,
        table_name: str,
        rows: List[Dict[str, Any]],
        batch_size: int = 500,
        max_retries: int = 5,
    ) -> Tuple[int, List[str]]:
        """
        Insert rows using streaming API.

        Includes retry logic for table not found errors (race condition
        when multiple functions create table simultaneously).
        """
        table_id = self.get_table_id(table_name)

        # Retry loop for getting the table (handles race condition)
        table = None
        for attempt in range(max_retries):
            try:
                table = self.client.get_table(table_id)
                break
            except NotFound:
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + random.random()
                    logger.warning(
                        f"Table {table_name} not found, retrying in {wait_time:.1f}s "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait_time)
                else:
                    raise

        total_inserted = 0
        errors = []

        # Process in batches with retry for streaming API consistency
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            batch_inserted = False

            # Retry loop for insert (handles streaming API eventual consistency)
            for attempt in range(max_retries):
                try:
                    insert_errors = self.client.insert_rows_json(table, batch)

                    if insert_errors:
                        # Check if it's a "table not found" error
                        first_error = str(insert_errors[0].get('errors', ''))
                        if 'not found' in first_error.lower() and attempt < max_retries - 1:
                            wait_time = (2 ** attempt) + random.random()
                            logger.warning(
                                f"Insert failed (table not ready), retrying in {wait_time:.1f}s "
                                f"(attempt {attempt + 1}/{max_retries})"
                            )
                            time.sleep(wait_time)
                            # Refresh table reference
                            table = self.client.get_table(table_id)
                            continue

                        for error in insert_errors:
                            error_msg = f"Row {error['index']}: {error['errors']}"
                            errors.append(error_msg)
                            logger.warning(f"Insert error: {error_msg}")
                    else:
                        total_inserted += len(batch)

                    batch_inserted = True
                    break

                except NotFound:
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) + random.random()
                        logger.warning(
                            f"Table not found during insert, retrying in {wait_time:.1f}s "
                            f"(attempt {attempt + 1}/{max_retries})"
                        )
                        time.sleep(wait_time)
                        # Refresh table reference
                        table = self.client.get_table(table_id)
                    else:
                        raise

            logger.debug(f"Inserted batch {i // batch_size + 1}: {len(batch)} rows")

        logger.info(
            f"Streaming insert complete: {total_inserted}/{len(rows)} rows",
            table=table_name,
            errors=len(errors),
        )

        return total_inserted, errors

    def delete_brand_data(
        self,
        brand_code: str,
        table_name: str,
        max_retries: int = 5,
    ) -> int:
        """
        Delete all rows for a specific brand from a table.

        Used for BA Produk categories which are daily snapshots.
        Note: Does NOT delete the header row (_brand_code = "_header_").

        Includes retry logic with exponential backoff for concurrent DML rate limits.
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

        last_error = None
        for attempt in range(max_retries):
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

            except BadRequest as e:
                # Check if it's a concurrent DML rate limit error
                error_msg = str(e)
                if "Too many DML statements" in error_msg or "concurrent" in error_msg.lower():
                    last_error = e
                    # Exponential backoff with jitter: 2^attempt + random(0-1) seconds
                    wait_time = (2 ** attempt) + random.random()
                    logger.warning(
                        f"Concurrent DML rate limit hit, retrying in {wait_time:.1f}s "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait_time)
                    continue
                else:
                    logger.warning(f"Failed to delete brand data: {e}")
                    return 0

            except Exception as e:
                logger.warning(f"Failed to delete brand data: {e}")
                return 0

        # All retries exhausted
        logger.error(f"Failed to delete brand data after {max_retries} retries: {last_error}")
        return 0

    def header_row_exists(self, table_name: str) -> bool:
        """Check if header row already exists in the table."""
        table_id = self.get_table_id(table_name)

        query = f"""
        SELECT COUNT(*) as cnt
        FROM `{table_id}`
        WHERE _brand_code = '_header_'
        """

        try:
            result = self.client.query(query).result()
            for row in result:
                return row.cnt > 0
        except Exception:
            return False

        return False

    def upload(
        self,
        parsed_file: ParsedFile,
        brand_code: str,
        import_id: str,
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Upload a parsed file to BigQuery.

        Format:
        - Row 1 (if not exists): Header row with _brand_code = "_header_"
        - Row 2+: Data rows with actual brand codes
        - Columns: _brand_code, A, B, C, ... (Excel-style)

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
            columns=len(parsed_file.headers),
        )

        try:
            # Ensure dataset exists
            self.ensure_dataset_exists()

            # Get schema based on number of columns
            schema = self.get_schema(len(parsed_file.headers))

            # Ensure table exists
            self.ensure_table_exists(category.bigquery_table, schema)

            # These categories are snapshots - DELETE existing brand data first
            # BA Dash is historical (append-only), so it's NOT in this list
            snapshot_categories = ["BA Produk", "Informasi", "Export SKU", "Demografis", "Proyeksi"]
            is_snapshot = any(cat in category.name for cat in snapshot_categories)

            if is_snapshot:
                deleted = self.delete_brand_data(brand_code, category.bigquery_table)
                logger.info(f"Deleted {deleted} existing rows for {brand_code} before import")

            # Prepare rows
            header_row, data_rows = self.prepare_rows(parsed_file, brand_code, import_id)

            # Check if we need to insert header row
            rows_to_insert = []
            if not self.header_row_exists(category.bigquery_table):
                rows_to_insert.append(header_row)
                logger.info("Inserting header row")

            rows_to_insert.extend(data_rows)

            # Upload using streaming insert
            rows_inserted, errors = self.streaming_insert(
                category.bigquery_table,
                rows_to_insert,
                batch_size=settings.BATCH_SIZE,
            )

            return len(errors) == 0, {
                "import_id": import_id,
                "table": category.bigquery_table,
                "rows_inserted": rows_inserted,
                "rows_total": len(rows_to_insert),
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
