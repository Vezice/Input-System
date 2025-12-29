////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Phase 4 Dashboard Setup.js
// Creates and configures the Dashboard and Brand Validation tabs
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Main setup function - Run this ONCE to create the dashboard tabs.
 * Creates:
 * 1. "Dashboard" tab - Overview of all category statuses
 * 2. "Brand Validation" tab - Tracks missing brands per category
 */
function AHA_SetupDashboard() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  // Create Dashboard tab
  let dashboardSheet = ss.getSheetByName("Dashboard");
  if (!dashboardSheet) {
    dashboardSheet = ss.insertSheet("Dashboard");
    Logger.log("Created 'Dashboard' sheet");
  }
  setupDashboardSheet(dashboardSheet);

  // Create Brand Validation tab
  let brandValSheet = ss.getSheetByName("Brand Validation");
  if (!brandValSheet) {
    brandValSheet = ss.insertSheet("Brand Validation");
    Logger.log("Created 'Brand Validation' sheet");
  }
  setupBrandValidationSheet(brandValSheet);

  // Create Brand Master tab (for IMPORTRANGE)
  let brandMasterSheet = ss.getSheetByName("Brand Master");
  if (!brandMasterSheet) {
    brandMasterSheet = ss.insertSheet("Brand Master");
    Logger.log("Created 'Brand Master' sheet");
  }
  setupBrandMasterSheet(brandMasterSheet);

  Logger.log("Dashboard setup complete!");
  SpreadsheetApp.getUi().alert("Dashboard setup complete!\n\nPlease update the IMPORTRANGE formula in 'Brand Master' sheet cell A2 to connect your external brand list.");
}


/**
 * Sets up the main Dashboard sheet with category overview
 */
function setupDashboardSheet(sheet) {
  sheet.clear();

  // Title
  sheet.getRange("A1").setValue("iBot System Dashboard").setFontSize(18).setFontWeight("bold");
  sheet.getRange("A2").setValue("Last Updated:").setFontWeight("bold");
  sheet.getRange("B2").setFormula('=NOW()');

  // Section: Category Status Overview
  sheet.getRange("A4").setValue("Category Status Overview").setFontSize(14).setFontWeight("bold").setBackground("#4285f4").setFontColor("white");
  sheet.getRange("A4:F4").merge();

  // Headers for category status
  const headers = ["Category", "Last Import", "Status", "Rows Imported", "Brands Found", "Missing Brands"];
  sheet.getRange("A5:F5").setValues([headers]).setFontWeight("bold").setBackground("#e8eaed");

  // Categories list
  const categories = [
    "BA Dash LAZ", "BA Dash SHO", "BA Dash TIK", "BA Dash TOK",
    "BA Iklan LAZ", "BA Iklan SHO", "BA Iklan TIK", "BA Iklan TOK",
    "BA Produk LAZ", "BA Produk SHO", "BA Produk TIK", "BA Produk TOK",
    "BA Promosi LAZ", "BA Promosi SHO", "BA Promosi TIK", "BA Promosi TOK",
    "Informasi Dasar SHO", "Informasi Dikirim Dalam SHO", "Informasi Media SHO", "Informasi Penjualan SHO",
    "Export SKU LAZ", "Export SKU TIK",
    "Demografis BSL", "Proyeksi Stok BSL"
  ];

  for (let i = 0; i < categories.length; i++) {
    const row = 6 + i;
    sheet.getRange(row, 1).setValue(categories[i]);
    sheet.getRange(row, 2).setValue("-"); // Last Import
    sheet.getRange(row, 3).setValue("Not Run"); // Status
    sheet.getRange(row, 4).setValue(0); // Rows Imported
    sheet.getRange(row, 5).setValue(0); // Brands Found
    sheet.getRange(row, 6).setValue(0); // Missing Brands
  }

  // Section: Recent Activity Log
  const logStartRow = 6 + categories.length + 2;
  sheet.getRange(logStartRow, 1).setValue("Recent Activity Log").setFontSize(14).setFontWeight("bold").setBackground("#4285f4").setFontColor("white");
  sheet.getRange(logStartRow, 1, 1, 6).merge();

  const logHeaders = ["Timestamp", "Category", "Action", "Details", "Status", ""];
  sheet.getRange(logStartRow + 1, 1, 1, 6).setValues([logHeaders]).setFontWeight("bold").setBackground("#e8eaed");

  // Format columns
  sheet.setColumnWidth(1, 180);
  sheet.setColumnWidth(2, 150);
  sheet.setColumnWidth(3, 100);
  sheet.setColumnWidth(4, 100);
  sheet.setColumnWidth(5, 100);
  sheet.setColumnWidth(6, 120);

  // Add conditional formatting for status
  const statusRange = sheet.getRange("C6:C" + (5 + categories.length));

  // Green for "Success"
  const successRule = SpreadsheetApp.newConditionalFormatRule()
    .whenTextEqualTo("Success")
    .setBackground("#b7e1cd")
    .setRanges([statusRange])
    .build();

  // Red for "Failed"
  const failedRule = SpreadsheetApp.newConditionalFormatRule()
    .whenTextEqualTo("Failed")
    .setBackground("#f4c7c3")
    .setRanges([statusRange])
    .build();

  // Yellow for "Running"
  const runningRule = SpreadsheetApp.newConditionalFormatRule()
    .whenTextEqualTo("Running")
    .setBackground("#fce8b2")
    .setRanges([statusRange])
    .build();

  sheet.setConditionalFormatRules([successRule, failedRule, runningRule]);

  Logger.log("Dashboard sheet configured");
}


/**
 * Sets up the Brand Validation sheet
 */
function setupBrandValidationSheet(sheet) {
  sheet.clear();

  // Title
  sheet.getRange("A1").setValue("Brand Validation Report").setFontSize(18).setFontWeight("bold");
  sheet.getRange("A2").setValue("This sheet tracks missing brands after each import").setFontStyle("italic");

  // Headers
  const headers = ["Category", "Expected Brands", "Found Brands", "Missing Count", "Missing Brands", "Last Checked"];
  sheet.getRange("A4:F4").setValues([headers]).setFontWeight("bold").setBackground("#e8eaed");

  // Categories
  const categories = [
    "BA Dash LAZ", "BA Dash SHO", "BA Dash TIK", "BA Dash TOK",
    "BA Iklan LAZ", "BA Iklan SHO", "BA Iklan TIK", "BA Iklan TOK",
    "BA Produk LAZ", "BA Produk SHO", "BA Produk TIK", "BA Produk TOK",
    "BA Promosi LAZ", "BA Promosi SHO", "BA Promosi TIK", "BA Promosi TOK",
    "Informasi Dasar SHO", "Informasi Dikirim Dalam SHO", "Informasi Media SHO", "Informasi Penjualan SHO",
    "Export SKU LAZ", "Export SKU TIK",
    "Demografis BSL", "Proyeksi Stok BSL"
  ];

  for (let i = 0; i < categories.length; i++) {
    const row = 5 + i;
    sheet.getRange(row, 1).setValue(categories[i]);
    sheet.getRange(row, 2).setValue(0); // Expected
    sheet.getRange(row, 3).setValue(0); // Found
    sheet.getRange(row, 4).setValue(0); // Missing Count
    sheet.getRange(row, 5).setValue("-"); // Missing Brands
    sheet.getRange(row, 6).setValue("-"); // Last Checked
  }

  // Format columns
  sheet.setColumnWidth(1, 200);
  sheet.setColumnWidth(2, 120);
  sheet.setColumnWidth(3, 120);
  sheet.setColumnWidth(4, 120);
  sheet.setColumnWidth(5, 300);
  sheet.setColumnWidth(6, 150);

  // Conditional formatting for missing count
  const missingRange = sheet.getRange("D5:D" + (4 + categories.length));

  // Red if > 0
  const missingRule = SpreadsheetApp.newConditionalFormatRule()
    .whenNumberGreaterThan(0)
    .setBackground("#f4c7c3")
    .setRanges([missingRange])
    .build();

  // Green if = 0
  const noMissingRule = SpreadsheetApp.newConditionalFormatRule()
    .whenNumberEqualTo(0)
    .setBackground("#b7e1cd")
    .setRanges([missingRange])
    .build();

  sheet.setConditionalFormatRules([missingRule, noMissingRule]);

  Logger.log("Brand Validation sheet configured");
}


/**
 * Sets up the Brand Master sheet with IMPORTRANGE placeholder
 * Structure: Column A = Marketplace Code (SHO, LAZ, TIK, TOK), Column B = Brand Code
 */
function setupBrandMasterSheet(sheet) {
  sheet.clear();

  // Instructions
  sheet.getRange("A1").setValue("Brand Master List").setFontSize(18).setFontWeight("bold");
  sheet.getRange("A3").setValue("Instructions:").setFontWeight("bold");
  sheet.getRange("A4").setValue("1. Replace the formula in row 10 with your actual IMPORTRANGE");
  sheet.getRange("A5").setValue("2. Format: =IMPORTRANGE(\"spreadsheet_id\", \"SheetName!A:B\")");
  sheet.getRange("A6").setValue("3. Column A = Marketplace Code (SHO, LAZ, TIK, TOK)");
  sheet.getRange("A7").setValue("4. Column B = Brand Code");
  sheet.getRange("A8").setValue("5. You may need to authorize the IMPORTRANGE connection first");

  // Headers for brand list
  sheet.getRange("A9").setValue("Marketplace Code").setFontWeight("bold").setBackground("#e8eaed");
  sheet.getRange("B9").setValue("Brand Code").setFontWeight("bold").setBackground("#e8eaed");

  // Placeholder IMPORTRANGE formula (as text, user will convert to formula)
  sheet.getRange("A10").setValue('=IMPORTRANGE("YOUR_SPREADSHEET_ID_HERE", "SheetName!A:B")').setFontStyle("italic").setFontColor("#666666");

  // Example data structure (will be replaced by IMPORTRANGE)
  sheet.getRange("A11").setValue("(Example: SHO)").setFontStyle("italic").setFontColor("#999999");
  sheet.getRange("B11").setValue("(Example: BRAND_A)").setFontStyle("italic").setFontColor("#999999");
  sheet.getRange("A12").setValue("(Example: LAZ)").setFontStyle("italic").setFontColor("#999999");
  sheet.getRange("B12").setValue("(Example: BRAND_A)").setFontStyle("italic").setFontColor("#999999");
  sheet.getRange("A13").setValue("(Example: TIK)").setFontStyle("italic").setFontColor("#999999");
  sheet.getRange("B13").setValue("(Example: BRAND_B)").setFontStyle("italic").setFontColor("#999999");

  sheet.setColumnWidth(1, 150);
  sheet.setColumnWidth(2, 200);

  Logger.log("Brand Master sheet configured with Marketplace Code + Brand Code structure");
}


/**
 * Updates the dashboard with latest category status
 * Called from Central sheets after import completes
 */
function AHA_UpdateDashboardStatus(category, status, rowCount, brandCount, missingCount) {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const dashboard = ss.getSheetByName("Dashboard");
    if (!dashboard) {
      Logger.log("Dashboard sheet not found");
      return;
    }

    // Find the category row
    const data = dashboard.getRange("A6:A30").getValues();
    for (let i = 0; i < data.length; i++) {
      if (data[i][0] === category) {
        const row = 6 + i;
        dashboard.getRange(row, 2).setValue(new Date()); // Last Import
        dashboard.getRange(row, 3).setValue(status); // Status
        dashboard.getRange(row, 4).setValue(rowCount); // Rows Imported
        dashboard.getRange(row, 5).setValue(brandCount); // Brands Found
        dashboard.getRange(row, 6).setValue(missingCount); // Missing Brands
        Logger.log(`Updated dashboard for ${category}`);
        break;
      }
    }
  } catch (e) {
    Logger.log(`Error updating dashboard: ${e.message}`);
  }
}


/**
 * Adds an entry to the activity log
 */
function AHA_LogActivity(category, action, details, status) {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const dashboard = ss.getSheetByName("Dashboard");
    if (!dashboard) return;

    // Find the log section (after categories)
    const logHeaderRow = findLogHeaderRow(dashboard);
    if (logHeaderRow === -1) return;

    // Insert new row after header
    dashboard.insertRowAfter(logHeaderRow + 1);
    const newRow = logHeaderRow + 2;

    dashboard.getRange(newRow, 1).setValue(new Date());
    dashboard.getRange(newRow, 2).setValue(category);
    dashboard.getRange(newRow, 3).setValue(action);
    dashboard.getRange(newRow, 4).setValue(details);
    dashboard.getRange(newRow, 5).setValue(status);

    // Keep only last 50 log entries
    const lastRow = dashboard.getLastRow();
    if (lastRow > logHeaderRow + 52) {
      dashboard.deleteRow(lastRow);
    }

  } catch (e) {
    Logger.log(`Error logging activity: ${e.message}`);
  }
}


/**
 * Helper to find the log header row
 */
function findLogHeaderRow(sheet) {
  const data = sheet.getRange("A1:A50").getValues();
  for (let i = 0; i < data.length; i++) {
    if (data[i][0] === "Recent Activity Log") {
      return i + 1; // Convert to 1-based row number
    }
  }
  return -1;
}
