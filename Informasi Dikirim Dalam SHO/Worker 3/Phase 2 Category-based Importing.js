////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Category-based Importing.gs
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// VSCode Tested
// Added non-brand data category
// Synchronized
// Mac Sync
// Retry

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
 * -- MODIFIED to standardize column order based on 'Type Validation' sheet --
 */
function AHA_ImportCategoryBatchInBatches2() {
    const start = new Date();
    try {
        const ss = SpreadsheetApp.getActiveSpreadsheet();
        const inputSheet = ss.getSheetByName(CONFIG.SHEET_NAMES.INPUT);
        if (!inputSheet) throw new Error(`Sheet '${CONFIG.SHEET_NAMES.INPUT}' not found.`);
        const logSheet = ss.getSheetByName(CONFIG.SHEET_NAMES.LOGS);
        if (!logSheet) throw new Error(`Sheet '${CONFIG.SHEET_NAMES.LOGS}' not found.`);
        const typeValidationSheet = ss.getSheetByName(CONFIG.SHEET_NAMES.TYPE_VALIDATION);
        if (!typeValidationSheet) throw new Error(`Sheet '${CONFIG.SHEET_NAMES.TYPE_VALIDATION}' not found.`);

        const headerRowIndicesFromSheet = typeValidationSheet.getRange(2, 2, typeValidationSheet.getLastRow() - 1, 1).getValues()
            .flat()
            .filter(rowIdx => typeof rowIdx === 'number' && rowIdx >= 1)
            .map(rowIdx => rowIdx - 1);
        const potentialHeaderRowsToCheck = [...new Set(headerRowIndicesFromSheet)].sort((a, b) => a - b);
        if (potentialHeaderRowsToCheck.length === 0) {
            potentialHeaderRowsToCheck.push(0);
        }

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

                            let categoryInfoForFile = { category: "Unknown", headerRowIndex: 1, dataRowIndex: 2 };
                            for (const rowIndexToCheck of potentialHeaderRowsToCheck) {
                                if (fileContentData[rowIndexToCheck] && Array.isArray(fileContentData[rowIndexToCheck])) {
                                    const result = AHA_GetCategory2(fileContentData[rowIndexToCheck], fileName);
                                    if (result.category !== "Unknown") {
                                        categoryInfoForFile = result;
                                        break; 
                                    }
                                }
                            }

                            let fileDataRowIndex = categoryInfoForFile.dataRowIndex;
                            let targetSheet = categorySheetCache[category];
                            if (!targetSheet) {
                                const tempCategorySheetName = `Temp ${category}`;
                                targetSheet = ss.getSheetByName(tempCategorySheetName) || ss.getSheetByName(AHA_AssignTempSheetToCategory2(category));
                                if (!targetSheet) throw new Error(`Could not get or assign temp sheet for category: ${category}`);
                                categorySheetCache[category] = targetSheet;
                            }
                            
                            // --- NEW LOGIC START ---
                            // Check if headers need to be set for the target sheet
                            if (targetSheet.getLastRow() === 0) {
                                const standardHeaders = AHA_GetStandardHeaders(category);
                                if (standardHeaders.length === 0) {
                                    throw new Error(`No standard headers found for category '${category}' in Type Validation sheet.`);
                                }

                                const processFunction = importDispatch[category];
                                let finalHeaders = [];
                                
                                // Add "Akun" column for brand-based files
                                if (processFunction !== AHA_ProcessGenericFileNoBrand) {
                                    finalHeaders = ["Akun", ...standardHeaders];
                                } else {
                                    finalHeaders = standardHeaders;
                                }

                                targetSheet.getRange(1, 1, 1, finalHeaders.length).setValues([finalHeaders])
                                    .setFontWeight("bold").setBackground("yellow");
                                targetSheet.getRange(2, 1, 1000, finalHeaders.length).setNumberFormat("@");
                                Logger.log(`Set standard header for '${category}'.`);
                            }
                            
                            // --- Create the column map FOR EVERY FILE ---
                            const fileHeaders = fileContentData[categoryInfoForFile.headerRowIndex - 1] || [];
                            const targetSheetHeaders = targetSheet.getRange(1, 1, 1, targetSheet.getLastColumn()).getValues()[0];
                            const standardHeadersForMapping = importDispatch[category] !== AHA_ProcessGenericFileNoBrand ? targetSheetHeaders.slice(1) : targetSheetHeaders;
                            
                            const columnMap = AHA_CreateColumnMap(fileHeaders, standardHeadersForMapping);
                            // --- NEW LOGIC END ---

                            const processFunction = importDispatch[category];
                            if (processFunction) {
                                // Pass the new columnMap to the processing function
                                importSuccess = processFunction(fileContentData, targetSheet, logSheet, folderName, fileDataRowIndex, columnMap);
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
 * Appends all data from a starting row after reordering columns based on the map.
 */
function AHA_ProcessGenericFile(data, targetSheet, logSheet, folderName, dataRowIndex, columnMap) {
  try {
    const startDataRow = dataRowIndex - 1;
    if (startDataRow >= data.length) return false;

    const content = data.slice(startDataRow).filter(row => row.some(cell => cell !== "" && cell != null));

    if (content.length > 0) {
      const standardColumnCount = targetSheet.getLastColumn() - 1; // Exclude the 'Akun' column
      const transformedData = [];

      for (const sourceRow of content) {
        const newRow = new Array(standardColumnCount).fill('');
        for (let i = 0; i < columnMap.length; i++) {
          const destIndex = columnMap[i];
          if (destIndex !== -1 && i < sourceRow.length) { 
            newRow[destIndex] = sourceRow[i];
          }
        }
        transformedData.push(newRow);
      }
      
      const folderCol = Array(transformedData.length).fill([folderName]);
      const targetRow = targetSheet.getLastRow() + 1;

      targetSheet.getRange(targetRow, 1, transformedData.length, 1).setValues(folderCol);
      targetSheet.getRange(targetRow, 2, transformedData.length, standardColumnCount).setValues(transformedData);
      return true;
    }
    return false;
  } catch (err) {
    Logger.log(`Error in AHA_ProcessGenericFile: ${err.message}`);
    return false;
  }
}


/**
 * Generic function for categories that DO NOT require a brand code column.
 * Imports data directly after reordering columns based on the map.
 */
function AHA_ProcessGenericFileNoBrand(data, targetSheet, logSheet, folderName, dataRowIndex, columnMap) {
  try {
    const startDataRow = dataRowIndex - 1;
    if (startDataRow >= data.length) return false;

    const content = data.slice(startDataRow).filter(row => row.some(cell => cell !== "" && cell != null));

    if (content.length > 0) {
      const standardColumnCount = targetSheet.getLastColumn();
      const transformedData = [];

      for (const sourceRow of content) {
        const newRow = new Array(standardColumnCount).fill('');
        for (let i = 0; i < columnMap.length; i++) {
          const destIndex = columnMap[i];
          if (destIndex !== -1 && i < sourceRow.length) {
            newRow[destIndex] = sourceRow[i];
          }
        }
        transformedData.push(newRow);
      }
      
      const targetRow = targetSheet.getLastRow() + 1;
      targetSheet.getRange(targetRow, 1, transformedData.length, standardColumnCount).setValues(transformedData);
      return true;
    }
    return false;
  } catch (err) {
    Logger.log(`Error in AHA_ProcessGenericFileNoBrand: ${err.message}`);
    return false;
  }
}

// --- FIX: All functions below now accept 'columnMap' ---

function AHA_ProcessBAProdukSHOFile(data, targetSheet, logSheet, folderName, dataRowIndex, columnMap) {
  return AHA_ProcessGenericFile(data, targetSheet, logSheet, folderName, dataRowIndex, columnMap);
}

/**
 * Special function for BA Dash SHO files that imports only one specific row.
 */
function AHA_ProcessBADashSHOFile(data, targetSheet, logSheet, folderName, dataRowIndex, columnMap) {
  try {
    // This function uses column mapping for robustness, even for a single row.
    const startDataRow = dataRowIndex - 1;
    if (startDataRow >= data.length) return false;

    const sourceRow = data[startDataRow];
    if (!sourceRow || !sourceRow.some(cell => cell !== "" && cell != null)) return false;
    
    const standardColumnCount = targetSheet.getLastColumn() - 1; // Exclude 'Akun'
    const transformedRow = new Array(standardColumnCount).fill('');

    for (let i = 0; i < columnMap.length; i++) {
        const destIndex = columnMap[i];
        if (destIndex !== -1 && i < sourceRow.length) {
            transformedRow[destIndex] = sourceRow[i];
        }
    }

    // Special logic to truncate the first column's data if it's a date string
    if (typeof transformedRow[0] === 'string' && transformedRow[0].length > 10) {
        transformedRow[0] = transformedRow[0].substring(0, 10);
    }

    const targetRow = targetSheet.getLastRow() + 1;
    targetSheet.getRange(targetRow, 1).setValue(folderName);
    targetSheet.getRange(targetRow, 2, 1, transformedRow.length).setValues([transformedRow]);
    return true;

  } catch (err) {
    Logger.log(`Error processing BA Dash SHO file: ${err.message}`);
    return false;
  }
}

function AHA_ProcessBAProdukLAZFile(data, targetSheet, logSheet, folderName, dataRowIndex, columnMap) {
  return AHA_ProcessGenericFile(data, targetSheet, logSheet, folderName, dataRowIndex, columnMap);
}

function AHA_ProcessBAProdukTIKFile(data, targetSheet, logSheet, folderName, dataRowIndex, columnMap) {
  return AHA_ProcessGenericFile(data, targetSheet, logSheet, folderName, dataRowIndex, columnMap);
}

/**
 * Special processing function for BA Dash LAZ files.
 * It reformats the date, truncates decimals from numbers, ignores percentages, and applies number formatting.
 */
function AHA_ProcessBADashLAZFile(data, targetSheet, logSheet, folderName, dataRowIndex, columnMap) {
    try {
        const startDataRow = dataRowIndex - 1;
        if (startDataRow >= data.length) return false;

        const sourceRow = data[startDataRow];
        if (!sourceRow || !sourceRow.some(cell => cell !== "" && cell != null)) return false;

        const standardColumnCount = targetSheet.getLastColumn() - 1;
        const transformedRow = new Array(standardColumnCount).fill('');
        
        // Reorder the data first using the map
        for (let i = 0; i < columnMap.length; i++) {
            const destIndex = columnMap[i];
            if (destIndex !== -1 && i < sourceRow.length) {
                transformedRow[destIndex] = sourceRow[i];
            }
        }

        // Now, apply special formatting to the ordered data
        const processedRow = [];
        for (let i = 0; i < transformedRow.length; i++) {
            let value = transformedRow[i];

            if (i === 0) { // Date formatting for the first data column
                try {
                    if (typeof value === 'string' && value.match(/^\d{4}-\d{2}-\d{2}$/)) {
                        const dateObject = new Date(value);
                        value = Utilities.formatDate(dateObject, Session.getScriptTimeZone(), "dd MMM yyyy");
                    }
                } catch (e) {
                    Logger.log(`Could not format date for BA Dash LAZ. Original value: '${value}'. Error: ${e.message}`);
                }
                processedRow.push(value);
                continue;
            }

            if (typeof value === 'string' && value.includes('%')) {
                processedRow.push(value);
                continue;
            }

            if (value !== null && value !== '' && !isNaN(value)) {
                value = parseInt(value, 10);
            }
            
            processedRow.push(value);
        }
        
        if (processedRow.length > 0) {
            const targetRow = targetSheet.getLastRow() + 1;
            
            targetSheet.getRange(targetRow, 1).setValue(folderName);
            targetSheet.getRange(targetRow, 2, 1, processedRow.length).setValues([processedRow]);
            
            if (processedRow.length > 1) { 
                const numberRange = targetSheet.getRange(targetRow, 3, 1, processedRow.length - 1);
                numberRange.setNumberFormat("#,##0");
            }
            
            return true;
        }
        return false;
    } catch (err) {
        Logger.log(`Error processing BA Dash LAZ file: ${err.message}`);
        return false;
    }
}

function AHA_ProcessBADashTIKFile(data, targetSheet, logSheet, folderName, dataRowIndex, columnMap) {
  return AHA_ProcessGenericFile(data, targetSheet, logSheet, folderName, dataRowIndex, columnMap);
}

function AHA_ProcessBADashTOKFile(data, targetSheet, logSheet, folderName, dataRowIndex, columnMap) {
  return AHA_ProcessGenericFile(data, targetSheet, logSheet, folderName, dataRowIndex, columnMap);
}

/**
 * Special function for Demografis BSL. It filters rows ending with "MON-M" in the
 * second column and moves them to a separate sheet.
 */
function AHA_ProcessDemografisBSLFile(data, targetSheet, logSheet, folderName, dataRowIndex, columnMap) {
    try {
        const startDataRow = dataRowIndex - 1;
        if (startDataRow >= data.length) return false;

        const content = data.slice(startDataRow).filter(row => row.some(cell => cell !== "" && cell != null));
        if (content.length === 0) return false;

        // Apply column mapping to all rows first
        const standardColumnCount = targetSheet.getLastColumn();
        const transformedContent = content.map(sourceRow => {
            const newRow = new Array(standardColumnCount).fill('');
            for (let i = 0; i < columnMap.length; i++) {
                const destIndex = columnMap[i];
                if (destIndex !== -1 && i < sourceRow.length) {
                    newRow[destIndex] = sourceRow[i];
                }
            }
            return newRow;
        });

        const defaultRows = [];
        const monRows = [];
        const MON_M_SUFFIX = "MON-M";

        // Now, filter the standardized rows
        for (const row of transformedContent) {
            const columnBValue = row[1];
            if (typeof columnBValue === 'string' && columnBValue.endsWith(MON_M_SUFFIX)) {
                monRows.push(row);
            } else {
                defaultRows.push(row);
            }
        }

        let importOccurred = false;

        if (defaultRows.length > 0) {
            const targetRow = targetSheet.getLastRow() + 1;
            targetSheet.getRange(targetRow, 1, defaultRows.length, defaultRows[0].length).setValues(defaultRows);
            importOccurred = true;
        }

        if (monRows.length > 0) {
            const ss = SpreadsheetApp.getActiveSpreadsheet();
            const monSheetName = 'Temp Demografis BSL MON-M';
            const monSheet = AHA_GetOrCreateSheet2(monSheetName);

            if (monSheet.getLastRow() === 0 && targetSheet.getLastRow() > 0) {
                const headerRange = targetSheet.getRange(1, 1, 1, targetSheet.getLastColumn());
                headerRange.copyTo(monSheet.getRange(1, 1));
            }

            const targetRow = monSheet.getLastRow() + 1;
            monSheet.getRange(targetRow, 1, monRows.length, monRows[0].length).setValues(monRows);
            importOccurred = true;
        }
        
        return importOccurred;

    } catch (err) {
        Logger.log(`Error in AHA_ProcessDemografisBSLFile: ${err.message}`);
        return false;
    }
}



// === CATEGORY-SPECIFIC IMPORT FUNCTIONS ===

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
  "Demografis BSL": AHA_ProcessDemografisBSLFile,
  "Proyeksi Stok BSL": AHA_ProcessGenericFileNoBrand
};

// === HELPER FUNCTIONS ===

/**
 * Converts an Excel/CSV file to a temporary Google Sheet.
 * @param {GoogleAppsScript.Drive.File} file The file to convert.
 * @returns {string|null} The ID of the new Google Sheet, or null on failure.
 */
function AHA_ConvertFileToGoogleSheet(file) {
  try {
    return AHA_ExecuteWithRetry(() => {
      const blob = file.getBlob();
      const resource = {
        title: `[TEMP CONVERSION] ${file.getName()}`,
        mimeType: MimeType.GOOGLE_SHEETS,
        parents: [{ id: file.getParents().next().getId() }]
      };
      // The Drive.Files.insert call is now protected by the retry wrapper.
      const newFile = Drive.Files.insert(resource, blob, { convert: true, supportsAllDrives: true });
      return newFile.id;
    }, `Convert File: ${file.getName()}`, 3, 3000);
  } catch (err) {
    Logger.log(`Error converting file ${file.getName()} after all retries: ${err.message}`);
    return null; // Return null on final failure
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
 * -- MODIFIED with a retry mechanism for sheet creation. --
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
          // --- NEW: Use the retry helper for the critical operation ---
          AHA_ExecuteWithRetry(() => {
            const sheet = AHA_GetOrCreateSheet2(name);
            sheet.clear(); // Ensure it's empty.
          }, `Create/Clear Temp Sheet ${name}`, 3, 3000); // Tries up to 3 times with a 3-second delay

          // This code only runs if the retry block succeeds
          controlSheet.getRange(i + 2, 2).setValue("Done");
          Logger.log(`✅ Created/Cleared temp sheet: ${name}`);
          AHA_SlackNotify3(`✅ *Completed*: Created/Cleared temp sheet - ${name}!`);

        } catch (err) {
          // This block runs only if ALL retries fail
          controlSheet.getRange(i + 2, 2).setValue("Error");
          Logger.log(`❌ Final Error creating ${name} after all retries: ${err.message}`);
          AHA_SlackNotify3(`❌ *Error*: Failed creating ${name} after all retries - ${err.message}! ${CONFIG.SLACK.MENTION_USER}`);
        }

        // --- Rescheduling logic remains the same ---
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

/**
 * Reads the 'Type Validation' sheet to get the standard, ordered list of headers for a given category.
 * @param {string} category The category to look up.
 * @returns {Array<string>} An array of standard header names.
 */
function AHA_GetStandardHeaders(category) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(CONFIG.SHEET_NAMES.TYPE_VALIDATION);
  if (!sheet) return [];

  const data = sheet.getRange("A2:Z" + sheet.getLastRow()).getValues();

  for (const row of data) {
    if (row[0] === category) {
      // Headers start from column E (index 4). Filter out any empty cells.
      return row.slice(4).filter(String);
    }
  }
  return []; // Return empty array if category is not found
}


/**
 * Compares the headers from an imported file to the standard headers and creates a map for reordering.
 * @param {Array<string>} fileHeaders The headers from the source file.
 *- * @param {Array<string>} standardHeaders The master list of headers from the validation sheet.
 * @returns {Array<number>} An array where the index is the source column and the value is the destination column index. -1 means the column should be ignored.
 */
function AHA_CreateColumnMap(fileHeaders, standardHeaders) {
  // Create a Map for quick, case-insensitive lookups of standard headers and their positions.
  const standardHeaderMap = new Map(
    standardHeaders.map((header, index) => [header.toString().toLowerCase().trim(), index])
  );
  
  const columnMap = new Array(fileHeaders.length);

  fileHeaders.forEach((header, sourceIndex) => {
    const standardizedFileHeader = (header || "").toString().toLowerCase().trim();
    if (standardHeaderMap.has(standardizedFileHeader)) {
      columnMap[sourceIndex] = standardHeaderMap.get(standardizedFileHeader);
    } else {
      // If the file's header is not in the standard list, mark it to be ignored.
      columnMap[sourceIndex] = -1;
    }
  });
  return columnMap;
}

