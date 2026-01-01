////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Phase 4 Control Panel.js
// Provides a button-based interface to run iBot commands directly from Google Sheets
// Use this when Slack is unavailable
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Creates a custom menu when the spreadsheet opens.
 * This allows running commands from the menu bar.
 */
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('iBot Commands')
    .addItem('Check Move Folders', 'CMD_CheckMove')
    .addItem('Check Failed Folders', 'CMD_CheckFailed')
    .addItem('Check Other Folders', 'CMD_CheckOther')
    .addSeparator()
    .addItem('Validate All Categories', 'CMD_ValidateAll')
    .addItem('Validate Specific Category...', 'CMD_ValidatePrompt')
    .addSeparator()
    .addItem('Show System Status', 'CMD_Status')
    .addItem('Show History', 'CMD_History')
    .addSeparator()
    .addSubMenu(ui.createMenu('Import: BA Dash')
      .addItem('BA Dash SHO', 'CMD_Import_BADashSHO')
      .addItem('BA Dash TIK', 'CMD_Import_BADashTIK')
      .addItem('BA Dash TOK', 'CMD_Import_BADashTOK')
      .addItem('BA Dash LAZ', 'CMD_Import_BADashLAZ'))
    .addSubMenu(ui.createMenu('Import: BA Produk')
      .addItem('BA Produk SHO', 'CMD_Import_BAProdukSHO')
      .addItem('BA Produk TIK', 'CMD_Import_BAProdukTIK')
      .addItem('BA Produk LAZ', 'CMD_Import_BAProdukLAZ'))
    .addSubMenu(ui.createMenu('Import: BA Iklan')
      .addItem('BA Iklan SHO', 'CMD_Import_BAIklanSHO')
      .addItem('BA Iklan TIK', 'CMD_Import_BAIklanTIK')
      .addItem('BA Iklan TOK', 'CMD_Import_BAIklanTOK')
      .addItem('BA Iklan LAZ', 'CMD_Import_BAIklanLAZ'))
    .addSubMenu(ui.createMenu('Import: BA Promosi')
      .addItem('BA Promosi SHO', 'CMD_Import_BAPromosiSHO')
      .addItem('BA Promosi TIK', 'CMD_Import_BAPromosiTIK')
      .addItem('BA Promosi TOK', 'CMD_Import_BAPromosiTOK')
      .addItem('BA Promosi LAZ', 'CMD_Import_BAPromosiLAZ'))
    .addSubMenu(ui.createMenu('Import: Informasi SHO')
      .addItem('Informasi Dasar SHO', 'CMD_Import_InformasiDasarSHO')
      .addItem('Informasi Dikirim Dalam SHO', 'CMD_Import_InformasiDikirimDalamSHO')
      .addItem('Informasi Media SHO', 'CMD_Import_InformasiMediaSHO')
      .addItem('Informasi Penjualan SHO', 'CMD_Import_InformasiPenjualanSHO'))
    .addSubMenu(ui.createMenu('Import: Export SKU')
      .addItem('Export SKU LAZ', 'CMD_Import_ExportSKULAZ')
      .addItem('Export SKU TIK', 'CMD_Import_ExportSKUTIK'))
    .addSubMenu(ui.createMenu('Import: BSL')
      .addItem('Demografis BSL', 'CMD_Import_DemografisBSL')
      .addItem('Proyeksi Stok BSL', 'CMD_Import_ProyeksiStokBSL'))
    .addItem('Import Custom Category...', 'CMD_ImportPrompt')
    .addSeparator()
    .addItem('Setup Control Panel Sheet', 'CMD_SetupControlPanel')
    .addToUi();
}

/**
 * Creates a Control Panel sheet with buttons and status display
 */
function CMD_SetupControlPanel() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName("Control Panel");

  if (!sheet) {
    sheet = ss.insertSheet("Control Panel");
    ss.setActiveSheet(sheet);
    ss.moveActiveSheet(1); // Move to first position
  }

  sheet.clear();

  // Title
  sheet.getRange("A1").setValue("iBot Control Panel").setFontSize(20).setFontWeight("bold");
  sheet.getRange("A2").setValue("Use the menu 'iBot Commands' or run functions below").setFontStyle("italic");
  sheet.getRange("A3").setValue("Last Updated:").setFontWeight("bold");
  sheet.getRange("B3").setFormula("=NOW()");

  // Section: Quick Actions
  sheet.getRange("A5").setValue("Quick Actions").setFontSize(14).setFontWeight("bold").setBackground("#4285f4").setFontColor("white");
  sheet.getRange("A5:D5").merge();

  const actions = [
    ["Check Move Folders", "CMD_CheckMove", "Check all 'Move' folders for files ready to import"],
    ["Check Failed Folders", "CMD_CheckFailed", "Check all 'Failed' folders for error files"],
    ["Check Other Folders", "CMD_CheckOther", "Check folders that aren't Move or Failed"],
    ["Validate All Categories", "CMD_ValidateAll", "Run brand validation on all BA categories"],
    ["Show System Status", "CMD_Status", "Display current status of all categories"],
    ["Show History", "CMD_History", "Display recent activity history"]
  ];

  sheet.getRange("A6:C6").setValues([["Action", "Function Name", "Description"]]).setFontWeight("bold").setBackground("#e8eaed");

  for (let i = 0; i < actions.length; i++) {
    const row = 7 + i;
    sheet.getRange(row, 1).setValue(actions[i][0]);
    sheet.getRange(row, 2).setValue(actions[i][1]).setFontFamily("Courier New");
    sheet.getRange(row, 3).setValue(actions[i][2]);
  }

  // Section: Import Categories
  const importStartRow = 7 + actions.length + 2;
  sheet.getRange(importStartRow, 1).setValue("Import Categories").setFontSize(14).setFontWeight("bold").setBackground("#34a853").setFontColor("white");
  sheet.getRange(importStartRow, 1, 1, 4).merge();

  sheet.getRange(importStartRow + 1, 1, 1, 3).setValues([["Category", "Function Name", "Description"]]).setFontWeight("bold").setBackground("#e8eaed");

  const categories = [
    ["BA Dash SHO", "CMD_Import_BADashSHO", "Import BA Dashboard SHO data"],
    ["BA Dash TIK", "CMD_Import_BADashTIK", "Import BA Dashboard TIK data"],
    ["BA Dash TOK", "CMD_Import_BADashTOK", "Import BA Dashboard TOK data"],
    ["BA Dash LAZ", "CMD_Import_BADashLAZ", "Import BA Dashboard LAZ data"],
    ["BA Produk SHO", "CMD_Import_BAProdukSHO", "Import BA Produk SHO data"],
    ["BA Produk TIK", "CMD_Import_BAProdukTIK", "Import BA Produk TIK data"],
    ["BA Produk LAZ", "CMD_Import_BAProdukLAZ", "Import BA Produk LAZ data"]
  ];

  for (let i = 0; i < categories.length; i++) {
    const row = importStartRow + 2 + i;
    sheet.getRange(row, 1).setValue(categories[i][0]);
    sheet.getRange(row, 2).setValue(categories[i][1]).setFontFamily("Courier New");
    sheet.getRange(row, 3).setValue(categories[i][2]);
  }

  // Section: How to Use
  const howToRow = importStartRow + 2 + categories.length + 2;
  sheet.getRange(howToRow, 1).setValue("How to Use").setFontSize(14).setFontWeight("bold").setBackground("#fbbc04").setFontColor("black");
  sheet.getRange(howToRow, 1, 1, 4).merge();

  const instructions = [
    "1. Use the menu: Extensions > Apps Script > Run (select function name)",
    "2. Or use the menu bar: iBot Commands > [Select Action]",
    "3. Check 'Command Log' sheet for results",
    "4. Check Apps Script execution logs for detailed output"
  ];

  for (let i = 0; i < instructions.length; i++) {
    sheet.getRange(howToRow + 1 + i, 1).setValue(instructions[i]);
  }

  // Format columns
  sheet.setColumnWidth(1, 200);
  sheet.setColumnWidth(2, 200);
  sheet.setColumnWidth(3, 400);

  SpreadsheetApp.getUi().alert("Control Panel created! Use menu 'iBot Commands' to run commands.");
}

// ============================================================
// COMMAND FUNCTIONS - These replace Slack commands
// ============================================================

/**
 * Check Move folders for files
 */
function CMD_CheckMove() {
  const ROOT_DRIVE_FOLDER_ID = '0AJyZWtXd1795Uk9PVA';
  logToCommandSheet("Running: Check Move Folders...");

  try {
    const result = handleFolderCheckDirect(ROOT_DRIVE_FOLDER_ID, 'Move');
    logToCommandSheet(result);
    SpreadsheetApp.getUi().alert("Check Move Complete!\n\n" + result.substring(0, 500));
  } catch (e) {
    logToCommandSheet("ERROR: " + e.message);
    SpreadsheetApp.getUi().alert("Error: " + e.message);
  }
}

/**
 * Check Failed folders
 */
function CMD_CheckFailed() {
  const ROOT_DRIVE_FOLDER_ID = '0AJyZWtXd1795Uk9PVA';
  logToCommandSheet("Running: Check Failed Folders...");

  try {
    const result = handleFolderCheckDirect(ROOT_DRIVE_FOLDER_ID, 'Failed');
    logToCommandSheet(result);
    SpreadsheetApp.getUi().alert("Check Failed Complete!\n\n" + result.substring(0, 500));
  } catch (e) {
    logToCommandSheet("ERROR: " + e.message);
    SpreadsheetApp.getUi().alert("Error: " + e.message);
  }
}

/**
 * Check Other folders
 */
function CMD_CheckOther() {
  const ROOT_DRIVE_FOLDER_ID = '0AJyZWtXd1795Uk9PVA';
  logToCommandSheet("Running: Check Other Folders...");

  try {
    const result = handleOtherFoldersCheckDirect(ROOT_DRIVE_FOLDER_ID);
    logToCommandSheet(result);
    SpreadsheetApp.getUi().alert("Check Other Complete!\n\n" + result.substring(0, 500));
  } catch (e) {
    logToCommandSheet("ERROR: " + e.message);
    SpreadsheetApp.getUi().alert("Error: " + e.message);
  }
}

/**
 * Validate all categories
 */
function CMD_ValidateAll() {
  logToCommandSheet("Running: Validate All Categories...");

  try {
    const result = handleValidateAllCommandDirect();
    logToCommandSheet(result);
    SpreadsheetApp.getUi().alert("Validate All Complete!\n\nCheck 'Command Log' sheet for full results.");
  } catch (e) {
    logToCommandSheet("ERROR: " + e.message);
    SpreadsheetApp.getUi().alert("Error: " + e.message);
  }
}

/**
 * Prompt user for category to validate
 */
function CMD_ValidatePrompt() {
  const ui = SpreadsheetApp.getUi();
  const response = ui.prompt(
    'Validate Category',
    'Enter category name (e.g., "BA Produk SHO", "BA Dash TIK L7D"):',
    ui.ButtonSet.OK_CANCEL
  );

  if (response.getSelectedButton() == ui.Button.OK) {
    const category = response.getResponseText().trim();
    if (category) {
      logToCommandSheet("Running: Validate " + category + "...");
      try {
        const result = handleBrandValidationDirect(category);
        logToCommandSheet(result);
        SpreadsheetApp.getUi().alert("Validation Complete!\n\n" + result.substring(0, 500));
      } catch (e) {
        logToCommandSheet("ERROR: " + e.message);
        SpreadsheetApp.getUi().alert("Error: " + e.message);
      }
    }
  }
}

/**
 * Show system status
 */
function CMD_Status() {
  logToCommandSheet("Running: Show Status...");

  try {
    const result = handleStatusCommandDirect();
    logToCommandSheet(result);
    SpreadsheetApp.getUi().alert("System Status:\n\n" + result.substring(0, 1000));
  } catch (e) {
    logToCommandSheet("ERROR: " + e.message);
    SpreadsheetApp.getUi().alert("Error: " + e.message);
  }
}

/**
 * Show history
 */
function CMD_History() {
  logToCommandSheet("Running: Show History...");

  try {
    const result = handleHistoryCommandDirect();
    logToCommandSheet(result);
    SpreadsheetApp.getUi().alert("Recent History:\n\n" + result.substring(0, 1000));
  } catch (e) {
    logToCommandSheet("ERROR: " + e.message);
    SpreadsheetApp.getUi().alert("Error: " + e.message);
  }
}

/**
 * Prompt user for custom category import
 */
function CMD_ImportPrompt() {
  const ui = SpreadsheetApp.getUi();
  const response = ui.prompt(
    'Import Category',
    'Enter category name exactly as it appears in the List sheet:',
    ui.ButtonSet.OK_CANCEL
  );

  if (response.getSelectedButton() == ui.Button.OK) {
    const category = response.getResponseText().trim();
    if (category) {
      triggerCategoryImport(category);
    }
  }
}

// Import shortcuts
// BA Dash
function CMD_Import_BADashSHO() { triggerCategoryImport("BA Dash SHO"); }
function CMD_Import_BADashTIK() { triggerCategoryImport("BA Dash TIK"); }
function CMD_Import_BADashTOK() { triggerCategoryImport("BA Dash TOK"); }
function CMD_Import_BADashLAZ() { triggerCategoryImport("BA Dash LAZ"); }

// BA Produk
function CMD_Import_BAProdukSHO() { triggerCategoryImport("BA Produk SHO"); }
function CMD_Import_BAProdukTIK() { triggerCategoryImport("BA Produk TIK"); }
function CMD_Import_BAProdukLAZ() { triggerCategoryImport("BA Produk LAZ"); }

// BA Iklan
function CMD_Import_BAIklanSHO() { triggerCategoryImport("BA Iklan SHO"); }
function CMD_Import_BAIklanTIK() { triggerCategoryImport("BA Iklan TIK"); }
function CMD_Import_BAIklanTOK() { triggerCategoryImport("BA Iklan TOK"); }
function CMD_Import_BAIklanLAZ() { triggerCategoryImport("BA Iklan LAZ"); }

// BA Promosi
function CMD_Import_BAPromosiSHO() { triggerCategoryImport("BA Promosi SHO"); }
function CMD_Import_BAPromosiTIK() { triggerCategoryImport("BA Promosi TIK"); }
function CMD_Import_BAPromosiTOK() { triggerCategoryImport("BA Promosi TOK"); }
function CMD_Import_BAPromosiLAZ() { triggerCategoryImport("BA Promosi LAZ"); }

// Informasi SHO
function CMD_Import_InformasiDasarSHO() { triggerCategoryImport("Informasi Dasar SHO"); }
function CMD_Import_InformasiDikirimDalamSHO() { triggerCategoryImport("Informasi Dikirim Dalam SHO"); }
function CMD_Import_InformasiMediaSHO() { triggerCategoryImport("Informasi Media SHO"); }
function CMD_Import_InformasiPenjualanSHO() { triggerCategoryImport("Informasi Penjualan SHO"); }

// Export SKU
function CMD_Import_ExportSKULAZ() { triggerCategoryImport("Export SKU LAZ"); }
function CMD_Import_ExportSKUTIK() { triggerCategoryImport("Export SKU TIK"); }

// BSL
function CMD_Import_DemografisBSL() { triggerCategoryImport("Demografis BSL"); }
function CMD_Import_ProyeksiStokBSL() { triggerCategoryImport("Proyeksi Stok BSL"); }

/**
 * Triggers a category import by calling the Central sheet's webhook
 */
function triggerCategoryImport(category) {
  const linksSheetName = "List";
  const categoryColumn = 1;
  const urlColumn = 7;

  logToCommandSheet("Triggering import for: " + category);

  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = ss.getSheetByName(linksSheetName);
    if (!sheet) throw new Error(`Sheet "${linksSheetName}" not found.`);

    const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, urlColumn).getValues();
    let targetUrl = null;

    for (let i = 0; i < data.length; i++) {
      if (data[i][categoryColumn - 1].toString().trim().toLowerCase() === category.toLowerCase()) {
        targetUrl = data[i][urlColumn - 1];
        break;
      }
    }

    if (!targetUrl) {
      throw new Error(`No URL found for category: "${category}"`);
    }

    // Create a minimal payload that mimics Slack
    const payload = `text=${encodeURIComponent(category)}&user_name=ControlPanel&response_url=`;

    const options = {
      'method': 'post',
      'contentType': 'application/x-www-form-urlencoded',
      'payload': payload,
      'muteHttpExceptions': true
    };

    Logger.log(`Triggering import for "${category}" via URL: ${targetUrl}`);
    const response = UrlFetchApp.fetch(targetUrl, options);
    const responseCode = response.getResponseCode();
    const responseText = response.getContentText();

    const result = `Import triggered for ${category}\nResponse: ${responseCode}\n${responseText}`;
    logToCommandSheet(result);

    SpreadsheetApp.getUi().alert(`Import Triggered!\n\nCategory: ${category}\nResponse: ${responseCode}\n\nCheck the category's Central sheet for progress.`);

  } catch (e) {
    logToCommandSheet("ERROR: " + e.message);
    SpreadsheetApp.getUi().alert("Error triggering import: " + e.message);
  }
}

// ============================================================
// DIRECT HANDLER FUNCTIONS (bypass Slack response_url)
// ============================================================

/**
 * Direct folder check handler - returns result string instead of sending to Slack
 */
function handleFolderCheckDirect(rootFolderId, targetFolderName) {
  const searchFolders = ["Move", "Failed"];
  const rootFolder = DriveApp.getFolderById(rootFolderId);
  const subFolders = rootFolder.getFolders();

  const targetFolders = [];

  while (subFolders.hasNext()) {
    const subFolder = subFolders.next();
    const subSubFolders = subFolder.getFolders();

    while (subSubFolders.hasNext()) {
      const folder = subSubFolders.next();
      const folderName = folder.getName();

      // Only look at Move and Failed folders, and only match the target
      if (searchFolders.includes(folderName) && folderName === targetFolderName) {
        const files = folder.getFiles();
        const fileList = [];
        while (files.hasNext()) {
          const file = files.next();
          // Skip FAILURE_LOG files for Failed folder check
          if (targetFolderName === "Failed" && file.getName().startsWith("FAILURE_LOG")) {
            continue;
          }
          fileList.push(file.getName());
        }

        if (fileList.length > 0) {
          targetFolders.push({
            parentName: subFolder.getName(),
            folderName: folderName,
            files: fileList
          });
        }
      }
    }
  }

  if (targetFolders.length === 0) {
    return `No files found in any '${targetFolderName}' folders.`;
  }

  let response = `Files in '${targetFolderName}' folders:\n\n`;
  for (const folder of targetFolders) {
    response += `*${folder.parentName}/${folder.folderName}*\n`;
    for (const file of folder.files) {
      response += `  - ${file}\n`;
    }
    response += "\n";
  }

  return response;
}

/**
 * Direct other folders check handler
 */
function handleOtherFoldersCheckDirect(rootFolderId) {
  const skipFolders = ["Move", "Failed", "Done"];
  const rootFolder = DriveApp.getFolderById(rootFolderId);
  const subFolders = rootFolder.getFolders();

  const otherFolders = [];

  while (subFolders.hasNext()) {
    const subFolder = subFolders.next();
    const subSubFolders = subFolder.getFolders();

    while (subSubFolders.hasNext()) {
      const folder = subSubFolders.next();
      const folderName = folder.getName();

      if (!skipFolders.includes(folderName)) {
        const files = folder.getFiles();
        const fileList = [];
        while (files.hasNext()) {
          fileList.push(files.next().getName());
        }

        if (fileList.length > 0) {
          otherFolders.push({
            parentName: subFolder.getName(),
            folderName: folderName,
            files: fileList
          });
        }
      }
    }
  }

  if (otherFolders.length === 0) {
    return "No files found in other folders (excluding Move, Failed, Done).";
  }

  let response = "Files in other folders:\n\n";
  for (const folder of otherFolders) {
    response += `*${folder.parentName}/${folder.folderName}*\n`;
    for (const file of folder.files) {
      response += `  - ${file}\n`;
    }
    response += "\n";
  }

  return response;
}

/**
 * Direct validate all handler
 */
function handleValidateAllCommandDirect() {
  const categories = [
    "BA Dash SHO", "BA Dash TIK", "BA Dash TOK", "BA Dash LAZ",
    "BA Produk SHO", "BA Produk TIK", "BA Produk LAZ"
  ];

  let results = "Validation Results:\n\n";

  for (const category of categories) {
    try {
      const result = AHA_ValidateBADashBrands(category);
      if (result.success) {
        const missingCount = result.missingBrands ? result.missingBrands.length : 0;
        const dupCount = result.duplicates ? result.duplicates.length : 0;
        results += `${category}: ${result.foundBrands.length} brands found`;
        if (missingCount > 0) results += `, ${missingCount} missing`;
        if (dupCount > 0) results += `, ${dupCount} duplicates`;
        results += "\n";
      } else {
        results += `${category}: ${result.message}\n`;
      }
    } catch (e) {
      results += `${category}: Error - ${e.message}\n`;
    }
  }

  return results;
}

/**
 * Direct brand validation handler
 */
function handleBrandValidationDirect(category) {
  try {
    const result = AHA_ValidateBADashBrands(category);

    if (!result.success) {
      return `Validation for ${category}: ${result.message}`;
    }

    let response = `*Validation: ${category}*\n`;
    response += `Found: ${result.foundBrands.length} brands\n`;
    response += `Expected: ${result.expectedBrands.length} brands\n`;

    if (result.missingBrands && result.missingBrands.length > 0) {
      response += `\nMissing (${result.missingBrands.length}): ${result.missingBrands.join(", ")}\n`;
    } else {
      response += `\nNo missing brands!\n`;
    }

    if (result.duplicates && result.duplicates.length > 0) {
      response += `\nDuplicates (${result.duplicates.length}):\n`;
      for (const dup of result.duplicates) {
        response += `  - ${dup.brand} on ${dup.date}: ${dup.count}x\n`;
      }
    }

    return response;

  } catch (e) {
    return `Error validating ${category}: ${e.message}`;
  }
}

/**
 * Direct status handler
 */
function handleStatusCommandDirect() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const dashboard = ss.getSheetByName("Dashboard");

  if (!dashboard) {
    return "Dashboard sheet not found. Run setup first.";
  }

  const data = dashboard.getRange("A6:F30").getValues();
  let status = "Category Status:\n\n";

  for (const row of data) {
    if (row[0]) {
      const category = row[0];
      const lastImport = row[1] || "-";
      const statusVal = row[2] || "Unknown";
      status += `${category}: ${statusVal} (Last: ${lastImport})\n`;
    }
  }

  return status;
}

/**
 * Direct history handler
 */
function handleHistoryCommandDirect() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const dashboard = ss.getSheetByName("Dashboard");

  if (!dashboard) {
    return "Dashboard sheet not found.";
  }

  // Find log section
  const allData = dashboard.getRange("A1:E50").getValues();
  let logStartRow = -1;

  for (let i = 0; i < allData.length; i++) {
    if (allData[i][0] === "Recent Activity Log") {
      logStartRow = i + 2; // Skip header row
      break;
    }
  }

  if (logStartRow === -1) {
    return "Activity log not found in Dashboard.";
  }

  let history = "Recent Activity (last 10):\n\n";
  const logData = dashboard.getRange(logStartRow + 1, 1, 10, 5).getValues();

  for (const row of logData) {
    if (row[0]) {
      const timestamp = row[0];
      const category = row[1];
      const action = row[2];
      history += `${timestamp}: ${category} - ${action}\n`;
    }
  }

  return history || "No recent activity found.";
}

// ============================================================
// UTILITY FUNCTIONS
// ============================================================

/**
 * Logs a message to the Command Log sheet
 */
function logToCommandSheet(message) {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let logSheet = ss.getSheetByName("Command Log");

    if (!logSheet) {
      logSheet = ss.insertSheet("Command Log");
      logSheet.getRange("A1:C1").setValues([["Timestamp", "Type", "Message"]]).setFontWeight("bold");
    }

    logSheet.insertRowAfter(1);
    logSheet.getRange(2, 1, 1, 3).setValues([[new Date(), "CMD", message.substring(0, 1000)]]);

    // Keep only last 100 entries
    const lastRow = logSheet.getLastRow();
    if (lastRow > 102) {
      logSheet.deleteRows(103, lastRow - 102);
    }

    Logger.log(message);

  } catch (e) {
    Logger.log("Could not write to Command Log: " + e.message);
  }
}
