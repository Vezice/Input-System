////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Sheet Setup.gs 
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// VSCode Tested
// Synchronized

/**
 * Copies data from a specified range in the "Input" sheet to a temporary sheet.
 * The temporary sheet's name is constructed from values in cells B1, C1, and D1.
 * @param {number} inputRow The number of rows to copy.
 * @param {number} inputCol The number of columns to copy.
 */
function AHA_CopyData2(inputRow, inputCol) {
  const start = new Date();
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet1 = ss.getSheetByName("Input");
    // Get parameters from specific cells in the "Input" sheet.
    const kode_mp = sheet1.getRange("B1").getValues();
    const kode_akun = sheet1.getRange("C1").getValues();
    const bulan = sheet1.getRange("D1").getValues();
    // Construct the target sheet name dynamically.
    const sheet2 = ss.getSheetByName("Temp " + kode_mp + " " + kode_akun + " " + bulan);

    // Get the range of raw data to be copied.
    const rawData = sheet1.getRange(3, 8, inputRow, inputCol).getValues();

    // Find the last row in the destination sheet to append data after it.
    let lastRow = sheet2.getLastRow();
    if (lastRow < 1) {
      lastRow = 1;
    }

    // Filter out completely empty rows from the source data.
    const data = rawData.filter(row => row[0] !== "" && row[0] != null);

    // Append the cleaned data to the destination sheet.
    sheet2.getRange(lastRow + 1, 1, data.length, inputCol).setValues(data);

    // The following block for checking duplicates is currently commented out.
    // It was intended to read column A of the destination sheet, identify duplicates,
    // and log them to the "Logs" sheet.
    // const dataSheet2 = sheet2.getRange("A2:A" + sheet2.getLastRow()).getValues().flat();
    // ...

  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}

/**
 * Removes duplicate rows from a sheet based on the values in Column A.
 * This function appears to be part of the older, manual workflow.
 */
function AHA_RemoveDuplicates2() {
  const start = new Date();
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet1 = ss.getSheetByName("Input");
    // Constructs the target sheet name from cells B1, C1, D1, same as AHA_CopyData2.
    const kode_mp = sheet1.getRange("B1").getValues();
    const kode_akun = sheet1.getRange("C1").getValues();
    const bulan = sheet1.getRange("D1").getValues();
    const sheet = ss.getSheetByName(kode_mp + " " + kode_akun + " " + bulan);

    const data = sheet.getRange('A2:A' + sheet.getLastRow()).getValues();
    const seen = {}; // Use an object to track values that have been seen.
    const rowsToDelete = [];
    const numRows = sheet.getLastRow();

    // First pass: Identify duplicate rows.
    if (numRows > 1) {
      for (let i = 0; i < data.length; i++) {
        const value = data[i][0];
        if (value && seen[value]) {
          // If value has been seen before, mark this row for deletion.
          rowsToDelete.push(i + 2); // +2 to account for 0-based index and starting at row 2.
        } else {
          seen[value] = true;
        }
      }
    }

    // Second pass: Clear the value in the first column of duplicate rows.
    // This step is somewhat redundant if the entire row is deleted later.
    if (numRows > 1) {
      for (let i = 0; i < rowsToDelete.length; i++) {
        sheet.getRange(rowsToDelete[i], 1).setValue('');
      }
    }

    // Third pass: Delete the entire row for each identified duplicate.
    // Looping backwards is important to avoid shifting row indices during deletion.
    if (numRows > 1) {
      for (let i = rowsToDelete.length - 1; i >= 0; i--) {
        const row = rowsToDelete[i];
        sheet.deleteRow(row);
      }
    }

    // Fourth pass: Consolidate data to remove any gaps.
    // This is also likely redundant given the deleteRow operation above.
    if (numRows > 1) {
      const newData = sheet.getRange('A2:A' + numRows).getValues();
      const nonEmptyRows = newData.filter(function(row) {
        return row[0] !== '';
      });
      sheet.getRange('A2:A' + numRows).clearContent();
      sheet.getRange('A2:A' + (nonEmptyRows.length + 1)).setValues(nonEmptyRows);
    } else {
      Logger.log("Data is Empty");
    }

  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}

/**
 * Helper function to clear specified areas (data, headers, formulas) on the "Input" sheet.
 * @param {string} inputArea The A1 notation for the data area to clear.
 * @param {string} headerArea The A1 notation for the header area to clear.
 * @param {string} formulaArea The A1 notation for the formula area to clear.
 */
function AHA_DeleteData2(inputArea, headerArea, formulaArea) {
  const start = new Date();
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet1 = ss.getSheetByName("Input");
    // Clear each specified range if it's provided.
    if (inputArea != null) {
      sheet1.getRange(inputArea).clearContent();
    }
    if (headerArea != null) {
      sheet1.getRange(headerArea).clearContent();
    }
    if (formulaArea != null) {
      sheet1.getRange(formulaArea).clearContent();
    }
  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}

/**
 * Prepares a single temporary sheet for a specific source sheet by copying its headers.
 * Note: This is part of an older workflow, different from the category-based system.
 * @param {string} sourceSheetName The name of the sheet to use as a template.
 * @returns {string} The name of the newly created temporary sheet.
 */
function AHA_PrepareTempSheet2(sourceSheetName) {
  const start = new Date();
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const tempPrefix = 'Temp ';
    const sourceSheet = ss.getSheetByName(sourceSheetName);
    if (!sourceSheet) {
      AHA_SlackNotify3("❌ *Error* : Source sheet " + sourceSheetName + " not found! <@U0A6B24777X>");
      throw new Error(`Source sheet '${sourceSheetName}' not found.`);
    }

    // Get headers from the source sheet.
    const headers = sourceSheet.getRange(1, 1, 1, sourceSheet.getLastColumn()).getValues()[0];
    const tempSheetName = tempPrefix + sourceSheetName;

    // Ensure no old temp sheet with the same name exists.
    const existingTemp = ss.getSheetByName(tempSheetName);
    if (existingTemp) ss.deleteSheet(existingTemp);

    // Create the new temp sheet.
    const tempSheet = AHA_CreateTempSheet2(tempSheetName);

    // Set and format the headers.
    tempSheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    const headerRange = tempSheet.getRange(1, 1, 1, headers.length);
    headerRange.setFontWeight("bold");

    // Format all data columns as Plain Text to prevent automatic data conversion.
    const formatRows = 1000;
    const rangeToFormat = tempSheet.getRange(2, 1, formatRows, headers.length);
    rangeToFormat.setNumberFormat("@");

    Logger.log(`✅ Temp sheet '${tempSheetName}' prepared.`);
    AHA_SlackNotify3("✅ *Completed* : Temp sheet " + tempSheetName + " prepared!");
    return tempSheetName;

  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}

/**
 * A specific implementation of prepareTempSheet, likely used as a fallback.
 * This is triggered when CATEGORY_IMPORTING_ENABLED is false.
 * @param {string} sourceSheetName The sheet to use as a template, defaulting to "BA Produk SHO".
 */
function AHA_TempSheetBAProdukSHO2(sourceSheetName = "BA Produk SHO") {
  const start = new Date();
  try {
    // This function is nearly identical to AHA_PrepareTempSheet2 and serves a similar purpose.
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const tempPrefix = 'Temp ';
    const sourceSheet = ss.getSheetByName(sourceSheetName);
    if (!sourceSheet) {
      AHA_SlackNotify3("❌ *Error* : Source sheet " + sourceSheetName + " not found! <@U0A6B24777X>");
      throw new Error(`Source sheet '${sourceSheetName}' not found.`);
    }

    const headers = sourceSheet.getRange(1, 1, 1, sourceSheet.getLastColumn()).getValues()[0];
    const tempSheetName = tempPrefix + sourceSheetName;

    const existingTemp = ss.getSheetByName(tempSheetName);
    if (existingTemp) ss.deleteSheet(existingTemp);

    const tempSheet = AHA_CreateTempSheet2(tempSheetName);

    tempSheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    const headerRange = tempSheet.getRange(1, 1, 1, headers.length);
    headerRange.setFontWeight("bold");
    const formatRows = 1000;
    const rangeToFormat = tempSheet.getRange(2, 1, formatRows, headers.length);
    rangeToFormat.setNumberFormat("@");

    AHA_SlackNotify3("✅ *Completed* : Temp sheet " + tempSheetName + " prepared!");
    Logger.log(`✅ Temp sheet '${tempSheetName}' prepared.`);
    return tempSheetName;

  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}

/**
 * Finalizes the process for the older workflow by deleting the original sheet
 * and renaming the temporary sheet to take its place.
 * @param {string} originalName The name of the original sheet.
 */
function AHA_FinalizeSheetSwap2(originalName = 'BA Produk SHO') {
  const start = new Date();
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const logSheet = ss.getSheetByName("Logs");
    const tempName = 'Temp ' + originalName;

    const tempSheet = ss.getSheetByName(tempName);
    const originalSheet = ss.getSheetByName(originalName);
    if (!tempSheet) throw new Error(`Temp sheet '${tempName}' not found.`);
    if (!originalSheet) throw new Error(`Original sheet '${originalName}' not found.`);

    // Delete the original sheet.
    ss.deleteSheet(originalSheet);

    // Rename the temp sheet to the final name.
    tempSheet.setName(originalName);

    AHA_SlackNotify3("✅ *Completed* : Sheet Temp " + originalName + " replaced the original!");
    Logger.log(`✅ Sheet Temp '${originalName}' replaced the original.`);
    logSheet.appendRow([new Date(), "Finalize Sheet Swap", "✅ Sheet Temp '${originalName}' replaced the original."]);

  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}

/**
 * A robust helper function to create a new temporary sheet.
 * It includes a retry mechanism in case of Google Sheets API errors.
 * @param {string} tempSheetName The name for the new sheet.
 * @returns {GoogleAppsScript.Spreadsheet.Sheet} The newly created sheet object.
 */
function AHA_CreateTempSheet2(tempSheetName) {
  const start = new Date();
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const logSheet = ss.getSheetByName("Logs");
    const tempSheetCheck = ss.getSheetByName(tempSheetName);
    if (tempSheetCheck) ss.deleteSheet(tempSheetCheck);

    let tempSheet;

    try {
      // First attempt to insert a new, lightweight sheet.
      tempSheet = ss.insertSheet(tempSheetName, { template: null });
      AHA_TrimSheetSize2(tempSheet); // Immediately trim it to reduce cell count.
    } catch (e) {
      AHA_SlackNotify3("❌ *Error* : Creating temp sheet failed - " + e.message + " <@U0A6B24777X>");
      Logger.log(`⚠️ First attempt failed: ${e.message} - Tolong hapus sheet "${tempSheetName}"`);
      logSheet.appendRow([new Date(), "Create Temp Sheet", `❌ First attempt failed. Tolong hapus sheet '${tempSheetName}'`]);

      // If the first attempt fails, wait and retry once.
      Utilities.sleep(5000); // 5-second pause.
      try {
        tempSheet = ss.insertSheet(tempSheetName, { template: null });
        AHA_TrimSheetSize2(tempSheet);
      } catch (retryError) {
        logSheet.appendRow([new Date(), "Create Temp Sheet", `❌ Second attempt failed. Tolong hapus sheet '${tempSheetName}'`]);
        throw new Error(`❌ Failed to create sheet '${tempSheetName}' after retry: ${retryError.message}`);
      }
    }
    return tempSheet;

  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}

/**
 * Helper function to immediately reduce the size of a newly created sheet.
 * This helps manage the total cell count of the spreadsheet.
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet The sheet to trim.
 */
function AHA_TrimSheetSize2(sheet) {
  const start = new Date();
  try {
    const maxRows = sheet.getMaxRows();
    const maxCols = sheet.getMaxColumns();

    // Reduce rows to a maximum of 100.
    if (maxRows > 100) {
      sheet.deleteRows(101, maxRows - 100);
    }
    // Reduce columns to a maximum of 26 (Z).
    if (maxCols > 26) {
      sheet.deleteColumns(27, maxCols - 26);
    }

  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}















