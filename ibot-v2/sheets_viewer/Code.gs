/**
 * iBot v2 BigQuery Viewer
 *
 * Apps Script to view BigQuery data in Google Sheets.
 * Add this to a Google Sheet via Extensions > Apps Script
 *
 * Required: Enable BigQuery API in Apps Script services
 */

// Configuration
const CONFIG = {
  PROJECT_ID: 'fbi-dev-484410',
  DATASET: 'ibot_v2_data',
  DEFAULT_LIMIT: 1000
};

// Available tables (categories)
const TABLES = {
  'BA Produk LAZ': 'ba_produk_laz',
  'BA Produk SHO': 'ba_produk_sho',
  'BA Produk TIK': 'ba_produk_tik',
  'BA Dash LAZ': 'ba_dash_laz',
  'BA Dash SHO': 'ba_dash_sho',
  'BA Dash TIK': 'ba_dash_tik',
  'BA Dash TOK': 'ba_dash_tok',
  'Informasi Dasar SHO': 'informasi_dasar_sho',
  'Informasi Dikirim SHO': 'informasi_dikirim_sho',
  'Informasi Media SHO': 'informasi_media_sho',
  'Informasi Penjualan SHO': 'informasi_penjualan_sho',
  'Export SKU LAZ': 'export_sku_laz',
  'Export SKU TIK': 'export_sku_tik',
  'Demografis BSL': 'demografis_bsl',
  'Proyeksi Stok BSL': 'proyeksi_stok_bsl'
};

/**
 * Create custom menu when spreadsheet opens
 */
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('iBot v2')
    .addItem('Load BA Produk LAZ', 'loadBAProdukLAZ')
    .addItem('Load BA Produk SHO', 'loadBAProdukSHO')
    .addItem('Load BA Produk TIK', 'loadBAProdukTIK')
    .addSeparator()
    .addItem('Load BA Dash LAZ', 'loadBADashLAZ')
    .addItem('Load BA Dash SHO', 'loadBADashSHO')
    .addItem('Load BA Dash TIK', 'loadBADashTIK')
    .addItem('Load BA Dash TOK', 'loadBADashTOK')
    .addSeparator()
    .addSubMenu(ui.createMenu('Shopee Data')
      .addItem('Informasi Dasar', 'loadInformasiDasar')
      .addItem('Informasi Dikirim', 'loadInformasiDikirim')
      .addItem('Informasi Media', 'loadInformasiMedia')
      .addItem('Informasi Penjualan', 'loadInformasiPenjualan'))
    .addSubMenu(ui.createMenu('Other')
      .addItem('Export SKU LAZ', 'loadExportSKULAZ')
      .addItem('Export SKU TIK', 'loadExportSKUTIK')
      .addItem('Demografis BSL', 'loadDemografisBSL')
      .addItem('Proyeksi Stok BSL', 'loadProyeksiStokBSL'))
    .addSeparator()
    .addItem('Custom Query...', 'showCustomQueryDialog')
    .addItem('Refresh Current Sheet', 'refreshCurrentSheet')
    .addToUi();
}

/**
 * Load data from BigQuery table to a sheet
 */
function loadTableToSheet(categoryName, options = {}) {
  const tableName = TABLES[categoryName];
  if (!tableName) {
    SpreadsheetApp.getUi().alert(`Unknown category: ${categoryName}`);
    return;
  }

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(categoryName);

  // Create sheet if it doesn't exist
  if (!sheet) {
    sheet = ss.insertSheet(categoryName);
  } else {
    sheet.clear();
  }

  // Build query
  const limit = options.limit || CONFIG.DEFAULT_LIMIT;
  const brandFilter = options.brand ? `AND _brand_code = '${options.brand}'` : '';

  const query = `
    SELECT *
    FROM \`${CONFIG.PROJECT_ID}.${CONFIG.DATASET}.${tableName}\`
    WHERE 1=1 ${brandFilter}
    LIMIT ${limit}
  `;

  try {
    const results = runBigQueryQuery(query);

    if (!results || results.length === 0) {
      sheet.getRange(1, 1).setValue('No data found');
      SpreadsheetApp.getUi().alert(`No data found in ${categoryName}`);
      return;
    }

    // Write headers
    const headers = Object.keys(results[0]);
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    sheet.getRange(1, 1, 1, headers.length)
      .setFontWeight('bold')
      .setBackground('#4285f4')
      .setFontColor('white');

    // Write data
    const data = results.map(row => headers.map(h => row[h] || ''));
    if (data.length > 0) {
      sheet.getRange(2, 1, data.length, headers.length).setValues(data);
    }

    // Auto-resize columns (first 10 only to save time)
    for (let i = 1; i <= Math.min(headers.length, 10); i++) {
      sheet.autoResizeColumn(i);
    }

    // Freeze header row
    sheet.setFrozenRows(1);

    // Add metadata
    const infoSheet = ss.getSheetByName('_Info') || ss.insertSheet('_Info');
    const now = new Date();
    const info = [
      ['Last Updated', now.toISOString()],
      ['Category', categoryName],
      ['Table', tableName],
      ['Row Count', results.length],
      ['Query Limit', limit]
    ];
    infoSheet.getRange(1, 1, info.length, 2).setValues(info);

    // Activate the data sheet
    sheet.activate();

    SpreadsheetApp.getUi().alert(
      `Loaded ${results.length} rows from ${categoryName}`
    );

  } catch (error) {
    SpreadsheetApp.getUi().alert(`Error loading data: ${error.message}`);
    Logger.log(error);
  }
}

/**
 * Run a BigQuery query and return results as array of objects
 */
function runBigQueryQuery(query) {
  const request = {
    query: query,
    useLegacySql: false
  };

  let queryResults = BigQuery.Jobs.query(request, CONFIG.PROJECT_ID);
  const jobId = queryResults.jobReference.jobId;

  // Wait for query to complete
  let sleepTimeMs = 500;
  while (!queryResults.jobComplete) {
    Utilities.sleep(sleepTimeMs);
    sleepTimeMs *= 2;
    queryResults = BigQuery.Jobs.getQueryResults(CONFIG.PROJECT_ID, jobId);
  }

  // Get all results (handle pagination)
  let rows = queryResults.rows || [];
  while (queryResults.pageToken) {
    queryResults = BigQuery.Jobs.getQueryResults(CONFIG.PROJECT_ID, jobId, {
      pageToken: queryResults.pageToken
    });
    rows = rows.concat(queryResults.rows || []);
  }

  if (rows.length === 0) {
    return [];
  }

  // Convert to array of objects
  const headers = queryResults.schema.fields.map(f => f.name);
  return rows.map(row => {
    const obj = {};
    row.f.forEach((cell, i) => {
      obj[headers[i]] = cell.v;
    });
    return obj;
  });
}

/**
 * Show custom query dialog
 */
function showCustomQueryDialog() {
  const html = HtmlService.createHtmlOutput(`
    <style>
      body { font-family: Arial, sans-serif; padding: 10px; }
      select, input, textarea { width: 100%; margin: 5px 0; padding: 8px; box-sizing: border-box; }
      button { background: #4285f4; color: white; padding: 10px 20px; border: none; cursor: pointer; margin-top: 10px; }
      button:hover { background: #3367d6; }
      label { font-weight: bold; margin-top: 10px; display: block; }
    </style>
    <label>Category:</label>
    <select id="category">
      ${Object.keys(TABLES).map(t => `<option value="${t}">${t}</option>`).join('')}
    </select>
    <label>Brand Code (optional):</label>
    <input type="text" id="brand" placeholder="e.g., GS, BRV">
    <label>Limit:</label>
    <input type="number" id="limit" value="1000">
    <button onclick="runQuery()">Load Data</button>
    <script>
      function runQuery() {
        const category = document.getElementById('category').value;
        const brand = document.getElementById('brand').value;
        const limit = document.getElementById('limit').value;
        google.script.run
          .withSuccessHandler(() => google.script.host.close())
          .loadTableWithOptions(category, brand, parseInt(limit));
      }
    </script>
  `)
  .setWidth(400)
  .setHeight(350);

  SpreadsheetApp.getUi().showModalDialog(html, 'Load BigQuery Data');
}

/**
 * Load table with custom options (called from dialog)
 */
function loadTableWithOptions(category, brand, limit) {
  loadTableToSheet(category, { brand, limit });
}

/**
 * Refresh current sheet (re-run last query)
 */
function refreshCurrentSheet() {
  const sheet = SpreadsheetApp.getActiveSheet();
  const categoryName = sheet.getName();

  if (TABLES[categoryName]) {
    loadTableToSheet(categoryName);
  } else {
    SpreadsheetApp.getUi().alert('Current sheet is not a data sheet. Please select a category from the menu.');
  }
}

// Quick load functions for menu items
function loadBAProdukLAZ() { loadTableToSheet('BA Produk LAZ'); }
function loadBAProdukSHO() { loadTableToSheet('BA Produk SHO'); }
function loadBAProdukTIK() { loadTableToSheet('BA Produk TIK'); }
function loadBADashLAZ() { loadTableToSheet('BA Dash LAZ'); }
function loadBADashSHO() { loadTableToSheet('BA Dash SHO'); }
function loadBADashTIK() { loadTableToSheet('BA Dash TIK'); }
function loadBADashTOK() { loadTableToSheet('BA Dash TOK'); }
function loadInformasiDasar() { loadTableToSheet('Informasi Dasar SHO'); }
function loadInformasiDikirim() { loadTableToSheet('Informasi Dikirim SHO'); }
function loadInformasiMedia() { loadTableToSheet('Informasi Media SHO'); }
function loadInformasiPenjualan() { loadTableToSheet('Informasi Penjualan SHO'); }
function loadExportSKULAZ() { loadTableToSheet('Export SKU LAZ'); }
function loadExportSKUTIK() { loadTableToSheet('Export SKU TIK'); }
function loadDemografisBSL() { loadTableToSheet('Demografis BSL'); }
function loadProyeksiStokBSL() { loadTableToSheet('Proyeksi Stok BSL'); }

/**
 * Get summary of all tables
 */
function getTableSummary() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName('Summary') || ss.insertSheet('Summary');
  sheet.clear();

  const headers = ['Category', 'Table Name', 'Row Count', 'Brands'];
  sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  sheet.getRange(1, 1, 1, headers.length)
    .setFontWeight('bold')
    .setBackground('#4285f4')
    .setFontColor('white');

  let row = 2;
  for (const [category, table] of Object.entries(TABLES)) {
    try {
      const query = `
        SELECT
          COUNT(*) as row_count,
          STRING_AGG(DISTINCT _brand_code, ', ' LIMIT 10) as brands
        FROM \`${CONFIG.PROJECT_ID}.${CONFIG.DATASET}.${table}\`
      `;
      const results = runBigQueryQuery(query);

      if (results && results.length > 0) {
        sheet.getRange(row, 1, 1, 4).setValues([[
          category,
          table,
          results[0].row_count || 0,
          results[0].brands || ''
        ]]);
      } else {
        sheet.getRange(row, 1, 1, 4).setValues([[category, table, 0, '']]);
      }
    } catch (e) {
      sheet.getRange(row, 1, 1, 4).setValues([[category, table, 'Error', e.message]]);
    }
    row++;
  }

  sheet.autoResizeColumns(1, headers.length);
  sheet.setFrozenRows(1);
  sheet.activate();
}
