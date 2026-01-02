////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Phase 3 BigQuery Export.js
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Main export function - called after category merge completes.
 * Exports the finalized category sheet data to BigQuery.
 *
 * This function is controlled by the BIGQUERY_ENABLED script property.
 * Set to "true" to enable exports, or "false" (default) to disable.
 */
function AHA_ExportToBigQuery3(category) {
  const props = PropertiesService.getScriptProperties();
  const enabled = props.getProperty("BIGQUERY_ENABLED");

  // Ghost code: Skip if BigQuery is not enabled
  if (enabled !== "true") {
    Logger.log("BigQuery export is disabled. Skipping.");
    return;
  }

  try {
    const projectId = props.getProperty("BIGQUERY_PROJECT_ID");
    const datasetId = props.getProperty("BIGQUERY_DATASET_ID");

    if (!projectId || !datasetId) {
      throw new Error("BigQuery project ID or dataset ID not configured in Script Properties.");
    }

    const tableId = AHA_CategoryToTableId3(category);

    // Get sheet data
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = ss.getSheetByName(category);
    if (!sheet) {
      throw new Error(`Sheet '${category}' not found`);
    }

    const data = sheet.getDataRange().getDisplayValues();
    if (data.length < 2) {
      Logger.log(`No data rows to export for ${category}`);
      return;
    }

    const headers = data[0];
    const rows = data.slice(1);

    // Ensure table exists with correct schema
    AHA_EnsureBigQueryTable3(projectId, datasetId, tableId, headers);

    // Export using streaming insert
    AHA_StreamingInsertToBigQuery3(projectId, datasetId, tableId, headers, rows, category);

    AHA_SlackNotify3(`📊 *BigQuery Export*: ${rows.length} rows exported to \`${tableId}\` for *${category}*`);

  } catch (err) {
    const errorMsg = `❌ BigQuery Export Error for ${category}: ${err.message}`;
    Logger.log(errorMsg);
    AHA_SlackNotify3(`${errorMsg} <@U0A6B24777X>`);
    // Don't throw - BigQuery failure shouldn't block main pipeline
  }
}


/**
 * Converts category name to BigQuery table ID (snake_case).
 * Example: "BA Dash TIK" -> "ba_dash_tik"
 */
function AHA_CategoryToTableId3(category) {
  return category
    .toLowerCase()
    .replace(/\s+/g, '_')
    .replace(/-/g, '_')
    .replace(/[^a-z0-9_]/g, '');
}


/**
 * Ensures BigQuery table exists, creates if needed.
 * Auto-generates schema from sheet headers.
 */
function AHA_EnsureBigQueryTable3(projectId, datasetId, tableId, headers) {
  try {
    BigQuery.Tables.get(projectId, datasetId, tableId);
    Logger.log(`Table ${tableId} already exists`);
  } catch (e) {
    if (e.message.includes("Not found")) {
      Logger.log(`Creating table ${tableId}...`);
      const schema = {
        fields: [
          { name: "import_timestamp", type: "TIMESTAMP", mode: "NULLABLE" },
          { name: "import_batch_id", type: "STRING", mode: "NULLABLE" },
          ...headers.map(h => ({
            name: AHA_HeaderToFieldName3(h),
            type: "STRING",
            mode: "NULLABLE"
          }))
        ]
      };

      const table = {
        tableReference: {
          projectId: projectId,
          datasetId: datasetId,
          tableId: tableId
        },
        schema: schema
      };

      BigQuery.Tables.insert(table, projectId, datasetId);
      Logger.log(`Table ${tableId} created successfully`);
    } else {
      throw e;
    }
  }
}


/**
 * Converts header to valid BigQuery field name.
 * - Lowercase
 * - Replace spaces with underscores
 * - Remove special characters
 * - Max 128 characters
 */
function AHA_HeaderToFieldName3(header) {
  return header
    .toLowerCase()
    .trim()
    .replace(/\s+/g, '_')
    .replace(/[^a-z0-9_]/g, '')
    .substring(0, 128) || 'column';
}


/**
 * Streaming insert rows to BigQuery (batched for performance).
 * Batches rows in groups of 500 to respect BigQuery limits.
 */
function AHA_StreamingInsertToBigQuery3(projectId, datasetId, tableId, headers, rows, category) {
  const batchSize = 500; // BigQuery recommends batches of 500
  const importTimestamp = new Date().toISOString();
  const batchId = `${category}_${Utilities.formatDate(new Date(), "Asia/Jakarta", "yyyyMMdd_HHmmss")}`;

  const fieldNames = headers.map(h => AHA_HeaderToFieldName3(h));

  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
    const insertRows = batch.map(row => {
      const json = {
        import_timestamp: importTimestamp,
        import_batch_id: batchId
      };
      row.forEach((cell, idx) => {
        if (fieldNames[idx]) {
          json[fieldNames[idx]] = cell !== null && cell !== undefined ? cell.toString() : "";
        }
      });
      return { json: json };
    });

    const request = {
      rows: insertRows,
      skipInvalidRows: true,
      ignoreUnknownValues: true
    };

    const response = BigQuery.Tabledata.insertAll(request, projectId, datasetId, tableId);

    if (response.insertErrors && response.insertErrors.length > 0) {
      Logger.log(`Insert errors (batch ${i}): ${JSON.stringify(response.insertErrors.slice(0, 3))}`);
    }
  }

  Logger.log(`Inserted ${rows.length} rows to ${tableId}`);
}


/**
 * One-time backfill function to export existing sheet data.
 * Run manually for each category by setting category in E1.
 */
function AHA_BackfillToBigQuery3() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const dashboard = ss.getSheetByName("System Dashboard");
  const category = dashboard.getRange("E1").getValue();

  if (!category) {
    Logger.log("No category set in E1");
    return;
  }

  Logger.log(`Starting backfill for ${category}...`);
  AHA_ExportToBigQuery3(category);
  Logger.log(`Backfill complete for ${category}`);
}


/**
 * Utility to backfill ALL categories (run once from Central).
 * Exports all existing sheet data to BigQuery with rate limiting.
 */
function AHA_BackfillAllCategoriesToBigQuery3() {
  const categories = [
    "BA Dash LAZ", "BA Dash SHO", "BA Dash TIK", "BA Dash TOK",
    "BA Iklan LAZ", "BA Iklan SHO", "BA Iklan TIK", "BA Iklan TOK",
    "BA Produk LAZ", "BA Produk SHO", "BA Produk TIK", "BA Produk TOK",
    "BA Promosi LAZ", "BA Promosi SHO", "BA Promosi TIK", "BA Promosi TOK"
  ];

  let successCount = 0;
  let errorCount = 0;

  for (const category of categories) {
    try {
      Logger.log(`Backfilling ${category}...`);
      AHA_ExportToBigQuery3(category);
      successCount++;
      Utilities.sleep(2000); // Rate limit protection
    } catch (e) {
      Logger.log(`Error backfilling ${category}: ${e.message}`);
      errorCount++;
    }
  }

  const summary = `📊 *BigQuery Backfill Complete*: ${successCount} categories exported, ${errorCount} errors.`;
  Logger.log(summary);
  AHA_SlackNotify3(summary);
}
