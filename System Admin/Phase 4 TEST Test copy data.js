/**
 * Copies all data from a source spreadsheet to this spreadsheet.
 * This is the efficient method using getValues() and setValues().
 */
function syncDataFromSource() {
  
  // --- START OF CONFIGURATION ---

  // 1. ID of the SOURCE spreadsheet (the one with the original data).
  //    Get this from the URL: docs.google.com/spreadsheets/d/SPREADSHEET_ID/edit
  const SOURCE_SPREADSHEET_ID = "1w2R6aQlWJ0-r_ehU-2NvevZVwhPMSNbvJ9V_L2wQy7k";

  // 2. Name of the specific sheet (tab) in the SOURCE file you want to copy from.
  const SOURCE_SHEET_NAME = "BA Produk SHO"; 

  // 3. Name of the specific sheet (tab) in THIS file where you want to paste the data.
  const TARGET_SHEET_NAME = "BA Produk SHO";

  // --- END OF CONFIGURATION ---

  try {
    // Open the source spreadsheet by its ID
    const sourceSpreadsheet = SpreadsheetApp.openById(SOURCE_SPREADSHEET_ID);
    const sourceSheet = sourceSpreadsheet.getSheetByName(SOURCE_SHEET_NAME);

    // Get the target sheet (it's in the current, active spreadsheet)
    const targetSpreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    const targetSheet = targetSpreadsheet.getSheetByName(TARGET_SHEET_NAME);

    if (!sourceSheet) {
      throw new Error(`Sheet "${SOURCE_SHEET_NAME}" not found in the source spreadsheet.`);
    }
    if (!targetSheet) {
      throw new Error(`Sheet "${TARGET_SHEET_NAME}" not found in this target spreadsheet.`);
    }

    // Read all data from the source sheet into memory in one call
    const data = sourceSheet.getDataRange().getValues();

    if (data.length === 0) {
      Logger.log("Source sheet is empty. Nothing to copy.");
      return;
    }

    // Clear the destination sheet to remove old data
    targetSheet.clearContents();

    // Write all data to the destination sheet in one single operation
    targetSheet.getRange(1, 1, data.length, data[0].length).setValues(data);

    Logger.log(`✅ Success! Copied ${data.length} rows from "${SOURCE_SHEET_NAME}" to "${TARGET_SHEET_NAME}".`);

  } catch (e) {
    // If an error occurs, log it for debugging.
    Logger.log(`❌ Error: ${e.message}`);
    // Optional: Send yourself an email or Slack notification on failure.
  }
}