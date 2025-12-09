////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Category-based Importing.gs
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// VSCode Tested
// Added non-brand data category
// Synchronized
// Mac Sync
// Retry
// Fi

/**
 * Assigns an available temporary sheet (e.g., "Temp A") to a specific category by renaming it.
 * -- MODIFIED to check the 'TempControl' sheet for "Done" status before assigning. --
 * @param {string} categoryName The name of the category needing a sheet.
 * @returns {string} The new name of the temporary sheet.
 */
function AHA_AssignTempSheetToCategory2(categoryName) {
  const start = new Date();
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    
    // --- NEW: Read the "source of truth" for temp sheet status ---
    const controlSheet = ss.getSheetByName("TempControl");
    if (!controlSheet) {
      throw new Error("❌ TempControl sheet is missing. Cannot assign temp sheets.");
    }
    
    const controlData = controlSheet.getRange(2, 1, controlSheet.getLastRow() - 1, 2).getValues();
    // Get a list of all sheets that are marked as "Done"
    const availableTempSheets = controlData
      .filter(row => row[1] === "Done")
      .map(row => row[0]); // e.g., ["Temp A", "Temp C"]

    if (availableTempSheets.length === 0) {
      AHA_SlackNotify3(`⚠️ *Warning*: No 'Done' Temp sheets available. Import may be delayed. ${CONFIG.SLACK.MENTION_USER}`);
      throw new Error("❌ No 'Done' Temp sheets are available to assign.");
    }

    // Now, check the "Done" sheets to find one that is truly available
    for (const temp of availableTempSheets) {
      const sheet = ss.getSheetByName(temp);
      
      // We check if the sheet exists AND still has its original generic name.
      // If it's marked "Done" but its name is already "Temp BA Produk SHO", we skip it.
      if (sheet && sheet.getName() === temp) {
        const newName = `Temp ${categoryName}`;
        sheet.clear(); // Ensure the sheet is empty before use.
        sheet.setName(newName);
        
        AHA_SlackNotify3(`✅ *Completed*: Assigned and cleared ${temp} for ${newName}`);
        Logger.log(`✅ Renamed ${temp} to ${newName}`);
        return newName; // Success!
      }
    }
    
    // If the loop finishes, all "Done" sheets were already in use.
    AHA_SlackNotify3(`❌ *Error*: All 'Done' Temp sheets are already in use. ${CONFIG.SLACK.MENTION_USER}`);
    throw new Error("❌ No available 'Done' Temp sheets to assign.");

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
/**
 * --- CORE BATCH PROCESSING (IMPORT) ---
 * -- MODIFIED to use advanced column mapping (SUM/COALESCE) --
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
            const specialRulesCache = {}; // Cache for special column rules

            for (const folderName in folderMap) {
                const folders = rootFolder.getFoldersByName(folderName);
                if (!folders.hasNext()) { /* ... (error handling) ... */ continue; }
                const folder = folders.next();

                for (const entry of folderMap[folderName]) {
                    const { fileName, rowIndex, category } = entry;
                    const fileIterator = folder.getFilesByName(fileName);
                    if (!fileIterator.hasNext()) { /* ... (error handling) ... */ continue; }
                    const file = fileIterator.next();
                    const maxRetries = 10;
                    const retryDelay = 10000;

                    for (let attempt = 0; attempt < maxRetries; attempt++) {
                        let importSuccess = false;
                        let isAnEmptyFile = false;
                        let tempConvertedFileId = null;
                        
                        try {
                            tempConvertedFileId = AHA_ConvertFileToGoogleSheet(file);
                            if (!tempConvertedFileId) {
                                throw new Error("File conversion to Google Sheet failed after all retries.");
                            }

                            const tempSpreadsheet = SpreadsheetApp.openById(tempConvertedFileId);
                            let tempConvertedSheet;
                            const sheetNameToImport = (category === "BA Dash SHO") ? "Pesanan Siap Dikirim" : null;
                            if (sheetNameToImport) tempConvertedSheet = tempSpreadsheet.getSheetByName(sheetNameToImport);
                            if (!tempConvertedSheet) tempConvertedSheet = tempSpreadsheet.getSheets()[0];

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
                            
                            // --- NEW MAPPING LOGIC START ---
                            const processFunction = importDispatch[category];
                            const standardHeaders = AHA_GetStandardHeaders(category);

                            if (targetSheet.getLastRow() === 0) {
                                if (standardHeaders.length === 0) {
                                    throw new Error(`No standard headers found for category '${category}' in Type Validation sheet.`);
                                }
                                let finalHeaders = (processFunction !== AHA_ProcessGenericFileNoBrand) ? ["Akun", ...standardHeaders] : standardHeaders;
                                targetSheet.getRange(1, 1, 1, finalHeaders.length).setValues([finalHeaders])
                                    .setFontWeight("bold").setBackground("yellow");
                                targetSheet.getRange(2, 1, 1000, finalHeaders.length).setNumberFormat("@");
                                Logger.log(`Set standard header for '${category}'.`);
                            }
                            
                            // Get file headers and create a lookup map
                            const fileHeaders = fileContentData[categoryInfoForFile.headerRowIndex - 1] || [];
                            const sourceHeaderMap = AHA_GetSourceHeaderIndices(fileHeaders);
                            
                            // Get special column rules for this category
                            if (!specialRulesCache[category]) {
                                specialRulesCache[category] = AHA_GetSpecialColumnRules(category);
                            }
                            const specialRules = specialRulesCache[category];
                            // --- NEW MAPPING LOGIC END ---

                            if (processFunction) {
                                // Pass the new mapping tools to the processing function
                                importSuccess = processFunction(
                                    fileContentData, 
                                    targetSheet, 
                                    logSheet, 
                                    folderName, 
                                    fileDataRowIndex, 
                                    standardHeaders, // <-- New param
                                    specialRules,    // <-- New param
                                    sourceHeaderMap  // <-- New param
                                );
                            } else {
                                Logger.log(`⚠️ No specific import function for category: ${category}. Skipping.`);
                                importSuccess = false;
                            }

                            if (importSuccess === false) { isAnEmptyFile = true; }

                            if (importSuccess || isAnEmptyFile) {
                                inputSheet.getRange(rowIndex, 4).setValue("Added");
                                const logMsg = `✅ Imported to ${targetSheet.getName()}` + (isAnEmptyFile ? " (0 rows added)" : "");
                                logSheet.appendRow([new Date(), fileName, logMsg]);
                                AHA_SlackNotify3(`✅ *Completed*: ${fileName} Imported to ${targetSheet.getName()}` + (isAnEmptyFile ? " (0 rows added)" : ""));
                                break; 
                            } else {
                                throw new Error(`Processing function for ${category} returned an unexpected error.`);
                            }

                        } catch (err) {
                            if (tempConvertedFileId) { try { DriveApp.getFileById(tempConvertedFileId).setTrashed(true); } catch (e) {} }
                            if (attempt < maxRetries - 1) {
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

        PropertiesService.getScriptProperties().setProperty("LAST_IMPORT_HEARTBEAT", new Date().getTime());

    } finally {
        const end = new Date();
        AHA_LogRuntime3(end - start);
    }
}

// === CATEGORY-SPECIFIC IMPORT FUNCTIONS ===
// All functions now use the new "pattern" based logic:
// SUM_STARTS_WITH: Finds all headers that start with a pattern, and sums them.
// COALESCE_STARTS_WITH: Finds all headers that start with a pattern, and takes the first one.

/**
 * Re-usable processor for all generic files.
 * -- MODIFIED to handle Duplicate Headers (First Come First Serve) --
 */
function AHA_ProcessAdvancedImport(data, targetSheet, folderName, dataRowIndex, standardHeaders, specialRules, sourceHeaderMap, includeBrandColumn) {
  try {
    const startDataRow = dataRowIndex - 1;
    if (startDataRow >= data.length) return false;

    // Filter out rows that are completely empty or just whitespace
    const content = data.slice(startDataRow).filter(row => row.some(cell => (cell || "").toString().trim() !== ""));
    if (content.length === 0) return false;
    
    const transformedData = [];
    const sourceHeaderList = Array.from(sourceHeaderMap.keys()); // Get all lowercase source headers

    // --- PRE-CALCULATE COLUMN MAPPING (The Fix) ---
    // We create a "working copy" of the map so we can shift() indices out of it 
    // without destroying the original map for other functions.
    const workingHeaderMap = new Map();
    sourceHeaderMap.forEach((indices, key) => {
        workingHeaderMap.set(key, [...indices]); // Clone the array of indices
    });

    // Determine exactly which source column index corresponds to each standard header
    const columnMapping = standardHeaders.map(header => {
        const key = header.toLowerCase().trim();
        if (workingHeaderMap.has(key)) {
            const indices = workingHeaderMap.get(key);
            if (indices.length > 0) {
                return indices.shift(); // TAKE the first available index (First Come, First Serve)
            }
        }
        return -1; // Not found via direct match
    });
    // ----------------------------------------------

    // Loop through each valid row of data from the source file
    for (const sourceRow of content) {
      const newRow = [];
      
      // Loop through the "Standard" headers
      for (let i = 0; i < standardHeaders.length; i++) {
        const standardHeader = standardHeaders[i];
        const directSourceIndex = columnMapping[i]; // Use our pre-calculated index
        let value = ''; 
        let found = false;

        // Check 1: Simple 1-to-1 match (using the specific index we reserved)
        if (directSourceIndex !== -1) {
          value = sourceRow[directSourceIndex];
          found = true;
        } 
        // Check 2: Special rules (SUM_STARTS_WITH or COALESCE_STARTS_WITH)
        else if (specialRules[standardHeader]) {
          const rule = specialRules[standardHeader];
          const action = rule.action;
          const pattern = (rule.replacements[0] || "").toLowerCase().trim();
          
          if (!pattern) continue; 

          if (action === "SUM_STARTS_WITH") {
            let sum = 0;
            const matchingHeaders = sourceHeaderList.filter(h => h.startsWith(pattern));
            for (const header of matchingHeaders) {
              // Loop through ALL columns associated with this header (in case of duplicates)
              const indices = sourceHeaderMap.get(header);
              for (const idx of indices) {
                const cellVal = Number(sourceRow[idx]);
                if (!isNaN(cellVal)) {
                    sum += cellVal;
                }
              }
            }
            value = sum;
            found = true;
          } 
          else if (action === "COALESCE_STARTS_WITH") {
            const matchingHeaders = sourceHeaderList.filter(h => h.startsWith(pattern));
            if (matchingHeaders.length > 0) {
              // Get the value from the *first* matching header's *first* column
              const firstIdx = sourceHeaderMap.get(matchingHeaders[0])[0];
              value = sourceRow[firstIdx];
              found = true;
            }
          }
        }
        
        newRow.push(value);
      }
      transformedData.push(newRow);
    }

    if (transformedData.length > 0) {
      const targetRow = targetSheet.getLastRow() + 1;
      if (includeBrandColumn) {
        const folderCol = Array(transformedData.length).fill([folderName]);
        targetSheet.getRange(targetRow, 1, transformedData.length, 1).setValues(folderCol);
        targetSheet.getRange(targetRow, 2, transformedData.length, standardHeaders.length).setValues(transformedData);
      } else {
        targetSheet.getRange(targetRow, 1, transformedData.length, standardHeaders.length).setValues(transformedData);
      }
      return true;
    }
    return false; 

  } catch (err) {
    Logger.log(`Error in AHA_ProcessAdvancedImport for ${folderName}: ${err.message}`);
    return false;
  }
}

// --- Wrapper Functions ---
// These functions now just call the main processor with the correct settings.

function AHA_ProcessGenericFile(data, targetSheet, logSheet, folderName, dataRowIndex, standardHeaders, specialRules, sourceHeaderMap) {
  return AHA_ProcessAdvancedImport(data, targetSheet, folderName, dataRowIndex, standardHeaders, specialRules, sourceHeaderMap, true);
}

function AHA_ProcessGenericFileNoBrand(data, targetSheet, logSheet, folderName, dataRowIndex, standardHeaders, specialRules, sourceHeaderMap) {
  return AHA_ProcessAdvancedImport(data, targetSheet, folderName, dataRowIndex, standardHeaders, specialRules, sourceHeaderMap, false);
}

function AHA_ProcessBAProdukSHOFile(data, targetSheet, logSheet, folderName, dataRowIndex, standardHeaders, specialRules, sourceHeaderMap) {
  return AHA_ProcessAdvancedImport(data, targetSheet, folderName, dataRowIndex, standardHeaders, specialRules, sourceHeaderMap, true);
}

function AHA_ProcessBAProdukLAZFile(data, targetSheet, logSheet, folderName, dataRowIndex, standardHeaders, specialRules, sourceHeaderMap) {
  return AHA_ProcessAdvancedImport(data, targetSheet, folderName, dataRowIndex, standardHeaders, specialRules, sourceHeaderMap, true);
}

function AHA_ProcessBAProdukTIKFile(data, targetSheet, logSheet, folderName, dataRowIndex, standardHeaders, specialRules, sourceHeaderMap) {
  return AHA_ProcessAdvancedImport(data, targetSheet, folderName, dataRowIndex, standardHeaders, specialRules, sourceHeaderMap, true);
}


// --- Special Single-Row Functions ---
// These functions must also be updated to the new signature and new logic.

/**
 * Special function for single-row reports (SHO, TIK, TOK).
 * -- MODIFIED to handle Array-based header map --
 */
function AHA_ProcessSingleRowReport(data, targetSheet, logSheet, folderName, dataRowIndex, standardHeaders, specialRules, sourceHeaderMap) {
  try {
    const startDataRow = dataRowIndex - 1;
    if (startDataRow >= data.length) return false;
    const sourceRow = data[startDataRow];
    if (!sourceRow || !sourceRow.some(cell => (cell || "").toString().trim() !== "")) return false;

    const transformedRow = [];
    const sourceHeaderList = Array.from(sourceHeaderMap.keys()); 

    for (const standardHeader of standardHeaders) {
        const standardHeaderLower = standardHeader.toLowerCase().trim();
        let value = '';
        let found = false;

        // Check 1: Simple 1-to-1 match
        if (sourceHeaderMap.has(standardHeaderLower)) {
            // FIX: Access [0] because the map now returns an array of indices
            const idx = sourceHeaderMap.get(standardHeaderLower)[0];
            value = sourceRow[idx];
            found = true;
        } 
        // Check 2: Special rules
        else if (specialRules[standardHeader]) {
            const rule = specialRules[standardHeader];
            const action = rule.action;
            const pattern = (rule.replacements[0] || "").toLowerCase().trim();

            if (!pattern) continue;

            if (action === "COALESCE_STARTS_WITH") {
                const matchingHeaders = sourceHeaderList.filter(h => h.startsWith(pattern));
                if (matchingHeaders.length > 0) {
                    // FIX: Access [0] here too
                    const idx = sourceHeaderMap.get(matchingHeaders[0])[0];
                    value = sourceRow[idx]; 
                    found = true;
                }
            }
        }
        transformedRow.push(value);
    }

    if (typeof transformedRow[0] === 'string' && transformedRow[0].length > 10) {
        transformedRow[0] = transformedRow[0].substring(0, 10);
    }
    const targetRow = targetSheet.getLastRow() + 1;
    targetSheet.getRange(targetRow, 1).setValue(folderName);
    targetSheet.getRange(targetRow, 2, 1, transformedRow.length).setValues([transformedRow]);
    return true;
  } catch (err) {
    Logger.log(`Error processing single-row report for ${folderName}: ${err.message}`);
    return false;
  }
}

/**
 * Special processor for BA Dash SHO.
 * - Truncates the double date string.
 * - Swaps DD-MM-YYYY to MM-DD-YYYY for system compatibility.
 */
function AHA_ProcessBADashSHOFile(data, targetSheet, logSheet, folderName, dataRowIndex, standardHeaders, specialRules, sourceHeaderMap) {
  try {
    const startDataRow = dataRowIndex - 1;
    if (startDataRow >= data.length) return false;
    const sourceRow = data[startDataRow];
    // Ensure the row isn't empty
    if (!sourceRow || !sourceRow.some(cell => (cell || "").toString().trim() !== "")) return false;

    const transformedRow = [];
    const sourceHeaderList = Array.from(sourceHeaderMap.keys()); 

    // 1. Standard Data Mapping
    for (const standardHeader of standardHeaders) {
        const standardHeaderLower = standardHeader.toLowerCase().trim();
        let value = '';

        if (sourceHeaderMap.has(standardHeaderLower)) {
            const idx = sourceHeaderMap.get(standardHeaderLower)[0];
            value = sourceRow[idx];
        } 
        else if (specialRules[standardHeader]) {
            const rule = specialRules[standardHeader];
            const action = rule.action;
            const pattern = (rule.replacements[0] || "").toLowerCase().trim();

            if (pattern && action === "COALESCE_STARTS_WITH") {
                const matchingHeaders = sourceHeaderList.filter(h => h.startsWith(pattern));
                if (matchingHeaders.length > 0) {
                    const idx = sourceHeaderMap.get(matchingHeaders[0])[0];
                    value = sourceRow[idx]; 
                }
            }
        }
        transformedRow.push(value);
    }

    // 2. Specific Date Formatting for SHO
    // Original format in file: "DD-MM-YYYY-DD-MM-YYYY" (e.g., 05-12-2025-05-12-2025)
    // Target format: "MM-DD-YYYY" (e.g., 12-05-2025)
    
    if (typeof transformedRow[0] === 'string' && transformedRow[0].length >= 10) {
        // A. Truncate to get the first date "05-12-2025"
        let dateStr = transformedRow[0].substring(0, 10);
        
        // B. Swap Day and Month
        // We assume the file is DD-MM-YYYY
        const parts = dateStr.split('-');
        if (parts.length === 3) {
            const dd = parts[0];
            const mm = parts[1];
            const yyyy = parts[2];
            
            // Reconstruct as MM-DD-YYYY
            transformedRow[0] = `${mm}-${dd}-${yyyy}`;
        } else {
             // Safety fallback: if it doesn't have 3 parts, leave the truncated version
             transformedRow[0] = dateStr;
        }
    }

    // 3. Write to Sheet
    const targetRow = targetSheet.getLastRow() + 1;
    targetSheet.getRange(targetRow, 1).setValue(folderName);
    targetSheet.getRange(targetRow, 2, 1, transformedRow.length).setValues([transformedRow]);
    
    return true;

  } catch (err) {
    Logger.log(`Error processing BA Dash SHO for ${folderName}: ${err.message}`);
    return false;
  }
}

function AHA_ProcessBADashTIKFile(data, targetSheet, logSheet, folderName, dataRowIndex, standardHeaders, specialRules, sourceHeaderMap) {
  return AHA_ProcessSingleRowReport(data, targetSheet, logSheet, folderName, dataRowIndex, standardHeaders, specialRules, sourceHeaderMap);
}
function AHA_ProcessBADashTOKFile(data, targetSheet, logSheet, folderName, dataRowIndex, standardHeaders, specialRules, sourceHeaderMap) {
  return AHA_ProcessSingleRowReport(data, targetSheet, logSheet, folderName, dataRowIndex, standardHeaders, specialRules, sourceHeaderMap);
}

function AHA_ProcessBADashLAZFile(data, targetSheet, logSheet, folderName, dataRowIndex, standardHeaders, specialRules, sourceHeaderMap) {
    try {
        const startDataRow = dataRowIndex - 1;
        if (startDataRow >= data.length) return false;
        const sourceRow = data[startDataRow];
        if (!sourceRow || !sourceRow.some(cell => (cell || "").toString().trim() !== "")) return false;

        const transformedRow = [];
        const sourceHeaderList = Array.from(sourceHeaderMap.keys());

        // 1. Map data based on standard headers
        for (const standardHeader of standardHeaders) {
            const standardHeaderLower = standardHeader.toLowerCase().trim();
            let value = '';
            let found = false;

            if (sourceHeaderMap.has(standardHeaderLower)) {
                const idx = sourceHeaderMap.get(standardHeaderLower)[0];
                value = sourceRow[idx];
                found = true;
            } else if (specialRules[standardHeader]) {
                const rule = specialRules[standardHeader];
                const action = rule.action;
                const pattern = (rule.replacements[0] || "").toLowerCase().trim();
                
                if (!pattern) continue;

                if (action === "COALESCE_STARTS_WITH") {
                    const matchingHeaders = sourceHeaderList.filter(h => h.startsWith(pattern));
                    if (matchingHeaders.length > 0) {
                        const idx = sourceHeaderMap.get(matchingHeaders[0])[0];
                        value = sourceRow[idx];
                        found = true;
                    }
                }
            }
            transformedRow.push(value);
        }

        // 2. Apply special formatting logic
        const processedRow = [];
        const conversionHeaderName = "Tingkat Konversi"; // The specific column to target

        for (let i = 0; i < transformedRow.length; i++) {
            let value = transformedRow[i];
            const currentHeader = standardHeaders[i];
            
            // Handle Date (Always the first column)
            if (i === 0) { 
                try {
                    if (typeof value === 'string' && value.match(/^\d{4}-\d{2}-\d{2}$/)) {
                        const dateObject = new Date(value);
                        value = Utilities.formatDate(dateObject, Session.getScriptTimeZone(), "dd MMM yyyy");
                    }
                } catch (e) {
                    Logger.log(`Could not format date for BA Dash LAZ. Error: ${e.message}`);
                }
                processedRow.push(value);
                continue;
            }

            // Handle existing Percentage strings
            if (typeof value === 'string' && value.includes('%')) {
                processedRow.push(value);
                continue;
            }

            // --- FIX START: Handle "Tingkat Konversi" ---
            // If this is the specific column, keep the decimal value (e.g. 0.01)
            // and skip the parseInt logic.
            if (currentHeader === conversionHeaderName) {
                processedRow.push(value); 
                continue;
            }
            // --- FIX END ---

            // Default: Convert all other numbers to integers
            if (value !== null && value !== '' && !isNaN(value)) { 
                value = parseInt(value, 10); 
            }
            processedRow.push(value);
        }
        
        // 3. Write data to sheet
        if (processedRow.length > 0) {
            const targetRow = targetSheet.getLastRow() + 1;
            targetSheet.getRange(targetRow, 1).setValue(folderName);
            targetSheet.getRange(targetRow, 2, 1, processedRow.length).setValues([processedRow]);
            
            // Apply standard integer formatting to columns 3 onwards
            if (processedRow.length > 1) { 
                const numberRange = targetSheet.getRange(targetRow, 3, 1, processedRow.length - 1);
                numberRange.setNumberFormat("#,##0");
            }

            // --- FIX START: Apply % Formatting to "Tingkat Konversi" ---
            // Calculate the exact column number for Tingkat Konversi
            const conversionIndex = standardHeaders.indexOf(conversionHeaderName);
            
            // Column 1 is FolderName, Column 2 is Data[0]... so Data[i] is at Column 2 + i
            if (conversionIndex !== -1) {
                const targetCol = 2 + conversionIndex;
                // Only override formatting if it falls within the data range
                if (targetCol >= 2) {
                     targetSheet.getRange(targetRow, targetCol).setNumberFormat("0.00%");
                     // 0.01 will now appear as "1.00%"
                }
            }
            // --- FIX END ---

            return true;
        }
        return false;
    } catch (err) {
        Logger.log(`Error processing BA Dash LAZ file: ${err.message}`);
        return false;
    }
}


function AHA_ProcessDemografisBSLFile(data, targetSheet, logSheet, folderName, dataRowIndex, standardHeaders, specialRules, sourceHeaderMap) {
    // This function uses the advanced processor, but with no brand column
    // This will NOT support the MON-M split logic anymore.
    Logger.log("WARNING: AHA_ProcessDemografisBSLFile is using advanced import. The MON-M split logic is disabled.");
    return AHA_ProcessAdvancedImport(data, targetSheet, folderName, dataRowIndex, standardHeaders, specialRules, sourceHeaderMap, false);
    
    // To re-enable the MON-M split, the logic from AHA_ProcessAdvancedImport
    // would need to be manually copied and merged into the old
    // AHA_ProcessDemografisBSLFile function, which is complex.
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
      .after(1 * 1000)
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
          // --- THIS IS THE FAIL-SAFE RETRY BLOCK ---
          AHA_ExecuteWithRetry(() => {
            const sheet = AHA_GetOrCreateSheet2(name);
            sheet.clear(); // Ensure it's empty.
          }, `Create/Clear Temp Sheet ${name}`, 3, 3000); // Tries up to 3 times

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

        // --- Rescheduling logic ---
        if (i < data.length - 1) {
          AHA_DeleteTriggers2("AHA_CreateNextTempSheet2");
          ScriptApp.newTrigger("AHA_CreateNextTempSheet2")
            .timeBased()
            .after(60 * 1000)
            .create();
        } else {
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
 * -- MODIFIED to dynamically read ALL columns, not just A:Z --
 * @param {string} category The category to look up.
 * @returns {Array<string>} An array of standard header names.
 */
function AHA_GetStandardHeaders(category) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(CONFIG.SHEET_NAMES.TYPE_VALIDATION);
  if (!sheet) return [];

  // --- FIX ---
  // The original code used .getRange("A2:Z" + sheet.getLastRow()), which hard-coded
  // the maximum column to 'Z'. This new code uses .getLastColumn() to
  // dynamically fetch all columns, ensuring no headers are missed.
  const data = sheet.getRange(
    2,                          // Starting row
    1,                          // Starting column (A)
    sheet.getLastRow() - 1,     // Number of rows (all data rows)
    sheet.getLastColumn()       // Number of columns (all available columns)
  ).getValues();
  // --- END FIX ---

  for (const row of data) {
    if (row[0] === category) {
      // Headers start from column E (index 4). Filter out any empty cells.
      return row.slice(4).filter(String);
    }
  }
  return []; // Return empty array if category is not found
}




/**
 * Reads the "Unique Column" sheet and builds a rulebook for a specific category.
 * @param {string} category The category to get rules for.
 * @returns {Object} A rulebook object. e.g.,
 * { "Standard Column Name": { action: "ActionType", replacements: ["Col A", "Col B"] } }
 */
function AHA_GetSpecialColumnRules(category) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const ruleSheet = ss.getSheetByName("Unique Column");
  if (!ruleSheet) {
    return {}; // No rule sheet, no rules.
  }
  
  const data = ruleSheet.getRange(2, 1, ruleSheet.getLastRow() - 1, 26).getValues();
  const rules = {};

  for (const row of data) {
    const rowCategory = row[0];
    if (rowCategory === category) {
      const standardColumn = row[1];
      const action = row[2];
      const replacements = row.slice(3).filter(String); // Get all replacement columns (D-Z)
      
      if (standardColumn && action && replacements.length > 0) {
        rules[standardColumn] = {
          action: action,
          replacements: replacements
        };
      }
    }
  }
  return rules;
}

/**
 * Creates a fast-lookup map of a file's headers.
 * -- MODIFIED to handle duplicate headers by storing an array of indices --
 * @param {Array<string>} fileHeaders The array of header names from the source file.
 * @returns {Map<string, Array<number>>} A map where key is lowercase header, value is ARRAY of original indices.
 */
function AHA_GetSourceHeaderIndices(fileHeaders) {
  const headerMap = new Map();
  fileHeaders.forEach((header, index) => {
    const standardizedHeader = (header || "").toString().toLowerCase().trim();
    // If key doesn't exist, create an empty array
    if (!headerMap.has(standardizedHeader)) {
      headerMap.set(standardizedHeader, []);
    }
    // Push the index into the array (Queueing it up)
    headerMap.get(standardizedHeader).push(index);
  });
  return headerMap;
}