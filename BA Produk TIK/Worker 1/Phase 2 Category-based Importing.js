////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Category-based Importing.gs
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// VSCode Tested
// Added non-brand data category
// Synchronized

/**
 * Assigns an available temporary sheet (e.g., "Temp A") to a specific category by renaming it.
 * @param {string} categoryName The name of the category needing a sheet.
 * @returns {string} The new name of the temporary sheet.
 */
function AHA_AssignTempSheetToCategory2(categoryName) {
  const start = new Date();
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const availableTempSheets = ['Temp A', 'Temp B', 'Temp C']; // Pool of available sheets.

    for (const temp of availableTempSheets) {
      const sheet = ss.getSheetByName(temp);
      // If the sheet exists and has its original generic name, it's available.
      if (sheet && sheet.getName() === temp) {
        const newName = `Temp ${categoryName}`;
        sheet.clear(); // Ensure the sheet is empty before use.
        sheet.setName(newName);
        AHA_SlackNotify3(`✅ *Completed*: Renamed and cleared ${temp} for ${newName}`);
        Logger.log(`✅ Renamed ${temp} to ${newName}`);
        return newName;
      }
    }
    // If the loop finishes, no sheets were available.
    AHA_SlackNotify3(`❌ *Error*: No available Temp sheets to assign. ${CONFIG.SLACK.MENTION_USER}`);
    throw new Error("❌ No available Temp sheets to assign.");

  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}

/**
 * --- SCRIPT INITIATION (IMPORT) ---
 * Main entry point for the category-based importing process.
 * It finds files ready for import and starts the batch processor.
 */
function AHA_ImportByCategoryBatch2() {
  const start = new Date();
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const inputSheet = ss.getSheetByName(CONFIG.SHEET_NAMES.INPUT);
    const data = inputSheet.getRange("B5:F" + inputSheet.getLastRow()).getValues();

    // Filter for rows that are ready to be imported.
    const validData = data.filter(row => {
      const folderName = String(row[0]).trim().toLowerCase();
      const status = row[2];
      const validation = row[4];
      return status === "Not Yet Added" && validation === "Validated" && folderName !== "failed";
    });

    const categories = [...new Set(validData.map(r => r[3]).filter(Boolean))];
    if (categories.length === 0) {
      Logger.log("No categories marked for import. Ending process.");
      return;
    }

    // This function now just kicks off the main batch processor.
    Logger.log("Found categories to import. Starting the main batch processor.");
    AHA_ImportCategoryBatchInBatches2();

  } catch (err) {
    Logger.log(`❌ Top-level error in ImportByCategoryBatch2: ${err.message}`);
    AHA_SlackNotify3(`❌ *Error*: Could not start import process - ${err.message} ${CONFIG.SLACK.MENTION_USER}`);
  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}

/**
 * --- CORE BATCH PROCESSING (IMPORT) ---
 * Processes a batch of files for import, with a 5-attempt retry mechanism for failures.
 */
function AHA_ImportCategoryBatchInBatches2() {
    const start = new Date();
    try {
        const ss = SpreadsheetApp.getActiveSpreadsheet();
        const inputSheet = ss.getSheetByName(CONFIG.SHEET_NAMES.INPUT);
        if (!inputSheet) throw new Error(`Sheet '${CONFIG.SHEET_NAMES.INPUT}' not found.`);
        const logSheet = ss.getSheetByName(CONFIG.SHEET_NAMES.LOGS);
        if (!logSheet) throw new Error(`Sheet '${CONFIG.SHEET_NAMES.LOGS}' not found.`);

        // --- NEW: Pre-fetch potential header rows, just like in the validation script ---
        const typeValidationSheet = ss.getSheetByName(CONFIG.SHEET_NAMES.TYPE_VALIDATION);
        if (!typeValidationSheet) throw new Error(`Sheet '${CONFIG.SHEET_NAMES.TYPE_VALIDATION}' not found.`);

        const headerRowIndicesFromSheet = typeValidationSheet.getRange(2, 2, typeValidationSheet.getLastRow() - 1, 1).getValues()
            .flat()
            .filter(rowIdx => typeof rowIdx === 'number' && rowIdx >= 1)
            .map(rowIdx => rowIdx - 1); // Convert to 0-based index
        const potentialHeaderRowsToCheck = [...new Set(headerRowIndicesFromSheet)].sort((a, b) => a - b);
        if (potentialHeaderRowsToCheck.length === 0) {
            potentialHeaderRowsToCheck.push(0);
        }
        // --- END NEW CODE ---

        const rootFolder = DriveApp.getFolderById(CONFIG.FOLDER_IDS.ROOT_SHARED_DRIVE);
        if (!rootFolder) throw new Error(`Root Drive folder not found by ID: ${CONFIG.FOLDER_IDS.ROOT_SHARED_DRIVE}.`);

        const batchSize = 40;
        const data = inputSheet.getRange("B5:F" + inputSheet.getLastRow()).getValues();
        const rowsToProcess = [];

        for (let i = 0; i < data.length && rowsToProcess.length < batchSize; i++) {
            const [folderNameRaw, fileName, status, category, validation] = data[i];
            if (status === "Not Yet Added" && validation === "Validated" && String(folderNameRaw).trim().toLowerCase() !== "failed") {
                if (folderNameRaw && fileName && category) {
                    rowsToProcess.push({ rowIndex: i + 5, folderName: folderNameRaw, fileName, category });
                }
            }
        }

        if (rowsToProcess.length > 0) {
            const folderMap = {};
            rowsToProcess.forEach(entry => {
                if (!folderMap[entry.folderName]) folderMap[entry.folderName] = [];
                folderMap[entry.folderName].push(entry);
            });
            const categorySheetCache = {};

            for (const folderName in folderMap) {
                const folders = rootFolder.getFoldersByName(folderName);
                if (!folders.hasNext()) {
                    AHA_SlackNotify3(`❌ *Error*: Source folder '${folderName}' not found! ${CONFIG.SLACK.MENTION_USER}`);
                    folderMap[folderName].forEach(entry => {
                        inputSheet.getRange(entry.rowIndex, 4).setValue("Import Failed");
                        logSheet.appendRow([new Date(), entry.fileName, `❌ Source folder '${folderName}' not found.`]);
                    });
                    continue;
                }
                const folder = folders.next();

                for (const entry of folderMap[folderName]) {
                    const { fileName, rowIndex, category } = entry;
                    const fileIterator = folder.getFilesByName(fileName);
                    if (!fileIterator.hasNext()) {
                        AHA_SlackNotify3(`❌ *Error*: File '${fileName}' not found in folder ${folderName}! ${CONFIG.SLACK.MENTION_USER}`);
                        inputSheet.getRange(rowIndex, 4).setValue("Import Failed");
                        logSheet.appendRow([new Date(), fileName, `❌ File not found in folder ${folderName}`]);
                        continue;
                    }
                    const file = fileIterator.next();
                    const maxRetries = 5;
                    const retryDelay = 10000;

                    for (let attempt = 0; attempt < maxRetries; attempt++) {
                        let importSuccess = false;
                        let tempConvertedFileId = null;
                        
                        try {
                            tempConvertedFileId = AHA_ConvertFileToGoogleSheet(file);
                            if (!tempConvertedFileId) throw new Error("Failed to convert file to Google Sheet.");

                            const tempSpreadsheet = SpreadsheetApp.openById(tempConvertedFileId);
                            let tempConvertedSheet;
                            const sheetNameToImport = (category === "BA Dash SHO") ? "Pesanan Siap Dikirim" : null;
                            if (sheetNameToImport) {
                                tempConvertedSheet = tempSpreadsheet.getSheetByName(sheetNameToImport);
                            }
                            if (!tempConvertedSheet) {
                                tempConvertedSheet = tempSpreadsheet.getSheets()[0];
                            }

                            const fileContentData = tempConvertedSheet.getDataRange().getValues();
                            DriveApp.getFileById(tempConvertedFileId).setTrashed(true);
                            tempConvertedFileId = null;

                            // --- MODIFIED: Loop through potential header rows to find the correct category info ---
                            let categoryInfoForFile = { category: "Unknown", headerRowIndex: 1, dataRowIndex: 2 }; // Default value
                            for (const rowIndexToCheck of potentialHeaderRowsToCheck) {
                                if (fileContentData[rowIndexToCheck] && Array.isArray(fileContentData[rowIndexToCheck])) {
                                    const result = AHA_GetCategory2(fileContentData[rowIndexToCheck], fileName);
                                    if (result.category !== "Unknown") {
                                        // We found a match! Use this info and stop searching.
                                        categoryInfoForFile = result;
                                        break; 
                                    }
                                }
                            }
                            // --- END MODIFICATION ---

                            let fileDataRowIndex = categoryInfoForFile.dataRowIndex;
                            let targetSheet = categorySheetCache[category];
                            if (!targetSheet) {
                                const tempCategorySheetName = `Temp ${category}`;
                                targetSheet = ss.getSheetByName(tempCategorySheetName) || ss.getSheetByName(AHA_AssignTempSheetToCategory2(category));
                                if (!targetSheet) throw new Error(`Could not get or assign temp sheet for category: ${category}`);
                                categorySheetCache[category] = targetSheet;
                            }

                            if (targetSheet.getLastRow() === 0) {
                                const headerRowIndex = categoryInfoForFile.headerRowIndex;
                                const headers = fileContentData[headerRowIndex - 1] || [];
                                const processFunction = importDispatch[category];

                                let totalHeaderCols = 0;

                                if (processFunction === AHA_ProcessGenericFileNoBrand) {
                                    if (headers.length > 0) {
                                        targetSheet.getRange(1, 1, 1, headers.length).setValues([headers]);
                                        totalHeaderCols = headers.length;
                                    }
                                } else {
                                    targetSheet.getRange(1, 1).setValue("Akun");
                                    if (headers.length > 0) {
                                        targetSheet.getRange(1, 2, 1, headers.length).setValues([headers]);
                                    }
                                    totalHeaderCols = headers.length + 1;
                                }
                                
                                if (totalHeaderCols > 0) {
                                    targetSheet.getRange(1, 1, 1, totalHeaderCols).setFontWeight("bold").setBackground("yellow");
                                    targetSheet.getRange(2, 1, 1000, totalHeaderCols).setNumberFormat("@");
                                }
                                Logger.log(`Set dynamic header for '${category}' from file '${fileName}'.`);
                            }

                            const processFunction = importDispatch[category];
                            if (processFunction) {
                                importSuccess = processFunction(fileContentData, targetSheet, logSheet, folderName, fileDataRowIndex);
                            } else {
                                Logger.log(`⚠️ No specific import function for category: ${category}. Skipping.`);
                                AHA_SlackNotify3(`⚠️ No import function for ${category}. Skipping ${fileName} ${CONFIG.SLACK.MENTION_USER}`);
                                importSuccess = false;
                            }

                            if (importSuccess) {
                                inputSheet.getRange(rowIndex, 4).setValue("Added");
                                logSheet.appendRow([new Date(), fileName, `✅ Imported to ${targetSheet.getName()}`]);
                                AHA_SlackNotify3(`✅ *Completed*: ${fileName} Imported to ${targetSheet.getName()}!`);
                                break;
                            } else {
                                throw new Error(`Processing function for ${category} returned false.`);
                            }

                        } catch (err) {
                            Logger.log(`Attempt ${attempt + 1}/${maxRetries} failed for ${fileName}: ${err.message}`);
                            
                            if (tempConvertedFileId) {
                                try { DriveApp.getFileById(tempConvertedFileId).setTrashed(true); } catch (e) {}
                            }

                            if (attempt < maxRetries - 1) {
                                AHA_SlackNotify3(`⚠️ Import failed for ${fileName} (Attempt ${attempt + 1}). Retrying in 10 seconds...`);
                                Utilities.sleep(retryDelay);
                            } else {
                                inputSheet.getRange(rowIndex, 4).setValue("Import Failed");
                                logSheet.appendRow([new Date(), fileName, `❌ Import Error after ${maxRetries} attempts: ${err.message}`]);
                                AHA_SlackNotify3(`❌ *Error*: Failed Import - ${fileName} after ${maxRetries} attempts: ${err.message} ${CONFIG.SLACK.MENTION_USER}`);
                                if (file) AHA_MoveFile2(file, "Failed");
                            }
                        }
                    }
                }
            }
        }

        const checkRange = inputSheet.getRange("B5:D" + inputSheet.getLastRow());
        const remainingData = checkRange.getValues();
        const actionableRemaining = remainingData.filter(row => {
            const folderName = String(row[0]).trim().toLowerCase();
            const status = row[2];
            return status === "Not Yet Added" && folderName !== "failed";
        });

        if (actionableRemaining.length > 0) {
            AHA_DeleteTriggers2("AHA_ImportCategoryBatchInBatches2");
            ScriptApp.newTrigger("AHA_ImportCategoryBatchInBatches2")
                .timeBased()
                .after(60 * 1000)
                .create();
            Logger.log(`${actionableRemaining.length} actionable files remain. Trigger set.`);
        } else {
            AHA_DeleteTriggers2("AHA_ImportCategoryBatchInBatches2");
            Logger.log("✅ All actionable files imported. Process complete.");
            logSheet.appendRow([new Date(), "Batch Import", "✅ Import finished."]);
            AHA_SlackNotify3("✅ *Process Complete*: All actionable files have been imported or marked as failed!");
            AHA_FinalizeAllTempSheets2();
        }
    } finally {
        const end = new Date();
        AHA_LogRuntime3(end - start);
    }
}

// === CATEGORY-SPECIFIC IMPORT FUNCTIONS ===

/**
 * Generic function for categories that require a brand code ("Akun") column.
 * Appends all data from a starting row.
 */
function AHA_ProcessGenericFile(data, targetSheet, logSheet, folderName, dataRowIndex) {
  try {
    const startDataRow = dataRowIndex - 1;
    if (startDataRow >= data.length) {
      return false; // No data to import.
    }
    const content = data.slice(startDataRow).filter(row => row[0] !== "" && row[0] != null);

    if (content.length > 0) {
      const targetRow = targetSheet.getLastRow() + 1;
      const folderCol = Array(content.length).fill([folderName]); // Create the brand code column.

      targetSheet.getRange(targetRow, 1, content.length, 1).setValues(folderCol);
      targetSheet.getRange(targetRow, 2, content.length, content[0].length).setValues(content);
      return true;
    }
    return false; // No content rows found.
  } catch (err) {
    Logger.log(`Error in AHA_ProcessGenericFile: ${err.message}`);
    return false;
  }
}

/**
 * Generic function for categories that DO NOT require a brand code column.
 * Imports data directly starting from column A.
 */
function AHA_ProcessGenericFileNoBrand(data, targetSheet, logSheet, folderName, dataRowIndex) {
  try {
    const startDataRow = dataRowIndex - 1;
    if (startDataRow >= data.length) {
      return false;
    }
    // Filter out completely empty rows.
    const content = data.slice(startDataRow).filter(row => row.some(cell => cell !== "" && cell != null));

    if (content.length > 0) {
      const targetRow = targetSheet.getLastRow() + 1;
      // Writes the data starting in column 1 (A).
      targetSheet.getRange(targetRow, 1, content.length, content[0].length).setValues(content);
      return true;
    }
    return false;
  } catch (err) {
    Logger.log(`Error in AHA_ProcessGenericFileNoBrand: ${err.message}`);
    return false;
  }
}

function AHA_ProcessBAProdukSHOFile(data, targetSheet, logSheet, folderName, dataRowIndex) {
  return AHA_ProcessGenericFile(data, targetSheet, logSheet, folderName, dataRowIndex);
}

/**
 * Special function for BA Dash SHO files that imports only one specific row.
 */
function AHA_ProcessBADashSHOFile(data, targetSheet, logSheet, folderName, dataRowIndex) {
  try {
    const targetRowIndexInFile = dataRowIndex - 1;
    const rowToImport = data[targetRowIndexInFile];

    if (rowToImport && rowToImport.length > 0 && (rowToImport[0] !== "" && rowToImport[0] != null)) {
      if (typeof rowToImport[0] === 'string' && rowToImport[0].length > 10) {
        rowToImport[0] = rowToImport[0].substring(0, 10);
      }
      const targetRow = targetSheet.getLastRow() + 1;
      targetSheet.getRange(targetRow, 1).setValue(folderName);
      targetSheet.getRange(targetRow, 2, 1, rowToImport.length).setValues([rowToImport]);
      return true;
    }
    return false;
  } catch (err) {
    Logger.log(`Error processing BA Dash SHO file: ${err.message}`);
    return false;
  }
}

function AHA_ProcessBAProdukLAZFile(data, targetSheet, logSheet, folderName, dataRowIndex) {
  return AHA_ProcessGenericFile(data, targetSheet, logSheet, folderName, dataRowIndex);
}

function AHA_ProcessBAProdukTIKFile(data, targetSheet, logSheet, folderName, dataRowIndex) {
  return AHA_ProcessGenericFile(data, targetSheet, logSheet, folderName, dataRowIndex);
}

/**
 * Special processing function for BA Dash LAZ files.
 * It truncates decimals from numbers, ignores percentages, and applies number formatting.
 */
function AHA_ProcessBADashLAZFile(data, targetSheet, logSheet, folderName, dataRowIndex) {
    try {
        const targetRowIndexInFile = dataRowIndex - 1;
        
        if (!data[targetRowIndexInFile]) {
             Logger.log(`Row ${dataRowIndex} not found in BA Dash LAZ file.`);
             return false;
        }

        const rowToImport = data[targetRowIndexInFile];
        const processedRow = [];

        // Loop through each cell in the row to process it
        for (const cell of rowToImport) {
            let value = cell;

            // Rule: Keep an eye out on the values with percentage (%) symbol. Don't touch them.
            if (typeof value === 'string' && value.includes('%')) {
                processedRow.push(value);
                continue; // Skip to the next cell
            }

            // Rule: Turn every number that was imported to always remove anything behind the dot.
            // Check if the value is a number or a string that can be converted to one.
            if (value !== null && value !== '' && !isNaN(value)) {
                // Use parseInt to get only the integer part, which effectively truncates any decimals.
                value = parseInt(value, 10);
            }
            
            processedRow.push(value);
        }
        
        if (processedRow.length > 0) {
            const targetRow = targetSheet.getLastRow() + 1;
            const range = targetSheet.getRange(targetRow, 2, 1, processedRow.length);
            
            // Set the brand code in the first column
            targetSheet.getRange(targetRow, 1).setValue(folderName);
            
            // Set the processed data
            range.setValues([processedRow]);
            
            // Rule: Make sure the format is a proper number format.
            // This applies the "12,345" style formatting to the cells.
            range.setNumberFormat("#,##0");
            
            return true;
        }
        return false;
    } catch (err) {
        Logger.log(`Error processing BA Dash LAZ file: ${err.message}`);
        return false;
    }
}

function AHA_ProcessBADashTIKFile(data, targetSheet, logSheet, folderName, dataRowIndex) {
  return AHA_ProcessGenericFile(data, targetSheet, logSheet, folderName, dataRowIndex);
}

function AHA_ProcessBADashTOKFile(data, targetSheet, logSheet, folderName, dataRowIndex) {
  return AHA_ProcessGenericFile(data, targetSheet, logSheet, folderName, dataRowIndex);
}

// === DISPATCH MAP: Routes categories to the correct processing function ===
const importDispatch = {
  "BA Produk SHO": AHA_ProcessBAProdukSHOFile,
  "BA Dash SHO": AHA_ProcessBADashSHOFile,
  "BA Produk LAZ": AHA_ProcessBAProdukLAZFile,
  "BA Produk TIK": AHA_ProcessBAProdukTIKFile,
  "BA Dash LAZ": AHA_ProcessBADashLAZFile,
  "BA Dash TIK": AHA_ProcessBADashTIKFile,
  "BA Dash TOK": AHA_ProcessBADashTOKFile,
  "Informasi Dasar SHO": AHA_ProcessGenericFile,
  "Informasi Penjualan SHO": AHA_ProcessGenericFile,
  "Informasi Media SHO": AHA_ProcessGenericFile,
  "Informasi Dikirim Dalam SHO": AHA_ProcessGenericFile,
  "Export SKU LAZ": AHA_ProcessGenericFile,
  "Export SKU TIK": AHA_ProcessGenericFile,
  "Demografis BSL": AHA_ProcessGenericFileNoBrand // Uses the new no-brand-code function.
};

// === HELPER FUNCTIONS ===

/**
 * Converts an Excel/CSV file to a temporary Google Sheet.
 * @param {GoogleAppsScript.Drive.File} file The file to convert.
 * @returns {string|null} The ID of the new Google Sheet, or null on failure.
 */
function AHA_ConvertFileToGoogleSheet(file) {
  try {
    const blob = file.getBlob();
    const resource = {
      title: `[TEMP CONVERSION] ${file.getName()}`,
      mimeType: MimeType.GOOGLE_SHEETS,
      parents: [{ id: file.getParents().next().getId() }]
    };
    const newFile = Drive.Files.insert(resource, blob, { convert: true, supportsAllDrives: true });
    return newFile.id;
  } catch (err) {
    Logger.log(`Error converting file ${file.getName()}: ${err.message}`);
    return null;
  }
}

/**
 * Finalizes the import process by renaming all "Temp" sheets to their permanent names.
 */
function AHA_FinalizeAllTempSheets2() {
  const start = new Date();
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const tempSheets = ss.getSheets().filter(s => s.getName().startsWith("Temp "));

    tempSheets.forEach(sheet => {
      const finalName = sheet.getName().replace("Temp ", "");
      const old = ss.getSheetByName(finalName);
      if (old) ss.deleteSheet(old); // Delete the old version.
      sheet.setName(finalName); // Promote the temp sheet.
      Logger.log(`✅ Finalized temp sheet ${finalName}`);
      AHA_SlackNotify3(`✅ *Completed*: Finalized temp sheet ${finalName}!`);
    });
  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}

/**
 * Starts the process of creating a pool of clean temporary sheets.
 */
function AHA_StartCreateTempSheetsBatch2() {
  const start = new Date();
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const controlSheet = AHA_GetOrCreateSheet2("TempControl");
    controlSheet.clear();
    controlSheet.appendRow(["Sheet Name", "Status"]);

    const tempSheets = ["Temp A", "Temp B", "Temp C"];
    tempSheets.forEach(name => {
      controlSheet.appendRow([name, "Pending"]);
    });

    AHA_DeleteTriggers2("AHA_CreateNextTempSheet2");
    ScriptApp.newTrigger("AHA_CreateNextTempSheet2")
      .timeBased()
      .after(5 * 1000)
      .create();
  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}

/**
 * Creates one temporary sheet from the control list and reschedules itself.
 */
function AHA_CreateNextTempSheet2() {
  const start = new Date();
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const controlSheet = ss.getSheetByName("TempControl");
    if (!controlSheet) return;
    const data = controlSheet.getRange(2, 1, controlSheet.getLastRow() - 1, 2).getValues();

    for (let i = 0; i < data.length; i++) {
      const [name, status] = data[i];
      if (status !== "Done") {
        try {
          const sheet = AHA_GetOrCreateSheet2(name);
          sheet.clear(); // Ensure it's empty.
          controlSheet.getRange(i + 2, 2).setValue("Done");
          Logger.log(`✅ Created/Cleared temp sheet: ${name}`);
          AHA_SlackNotify3(`✅ *Completed*: Created/Cleared temp sheet - ${name}!`);
        } catch (err) {
          controlSheet.getRange(i + 2, 2).setValue("Error");
          Logger.log(`❌ Error creating ${name}: ${err.message}`);
          AHA_SlackNotify3(`❌ *Error*: Failed creating ${name} - ${err.message}! ${CONFIG.SLACK.MENTION_USER}`);
        }

        // If there are more sheets to create, reschedule.
        if (i < data.length - 1) {
          AHA_DeleteTriggers2("AHA_CreateNextTempSheet2");
          ScriptApp.newTrigger("AHA_CreateNextTempSheet2")
            .timeBased()
            .after(60 * 1000)
            .create();
        } else {
          // Otherwise, the process is complete.
          AHA_DeleteTriggers2("AHA_CreateNextTempSheet2");
          Logger.log("✅ All temp sheets prepared.");
          AHA_SlackNotify3("✅ *Completed*: All temp sheets prepared!");
        }
        return; // Process one sheet per execution.
      }
    }
    AHA_DeleteTriggers2("AHA_CreateNextTempSheet2");
  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}

/**
 * Utility function to get a sheet by name, or create it if it doesn't exist.
 */
function AHA_GetOrCreateSheet2(sheetName) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(sheetName);
  if (!sheet) sheet = ss.insertSheet(sheetName);
  return sheet;
}

/**
 * Utility function to delete all triggers for a given function name.
 */
function AHA_DeleteTriggers2(functionName) {
  const triggers = ScriptApp.getProjectTriggers();
  triggers.forEach(trigger => {
    if (trigger.getHandlerFunction() === functionName) {
      ScriptApp.deleteTrigger(trigger);
    }
  });
}

/**
 * Utility function to clean up unused temporary sheets after a run.
 */
function AHA_DeleteUnusedTempSheets2() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const tempSheetNames = ["A", "B", "C", "TempControl"];
  tempSheetNames.forEach(name => {
    const sheet = ss.getSheetByName(name);
    if (sheet) {
      ss.deleteSheet(sheet);
      Logger.log(`🗑️ Deleted unused temp sheet: ${name}`);
      AHA_SlackNotify3(`🗑️ Deleted unused temp sheet: ${name}`);
    }
  });
}



