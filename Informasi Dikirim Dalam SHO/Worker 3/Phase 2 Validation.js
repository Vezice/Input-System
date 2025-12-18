////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Validation.gs 
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Synchronized

// --- CONFIGURATION OBJECT ---
// Central place to store all settings and magic strings for easy management.
const CONFIG = {
  BATCH_SIZE: 10, // The number of files to process in a single execution run to avoid timeouts.
  CATEGORY_IMPORTING_ENABLED: true, // A feature flag to enable or disable category importing.
  FAILURE_LOG_DOC_ID: "1sEqcKalRpWOakM82Ffr-BRNN17ft4wUnv31fSIuQZYw",
  SHEET_NAMES: { // Standardizes sheet names used throughout the script.
    INPUT: "Input",
    LOGS: "Logs",
    TYPE_VALIDATION: "Type Validation"
    // REMOVED CATEGORY_VALIDATION from here. It is now fetched at runtime.
  },
  FOLDER_IDS: { // Stores important Google Drive folder IDs.
    ROOT_SHARED_DRIVE: "0AJyZWtXd1795Uk9PVA", // The main shared drive where destination folders are located.
    MOVE_FOLDER: "1zQEPDi4dI3gUJiEKYp-GQI8lfNFwL1sh" // The base folder where files to be processed are initially located.
  },
  SLACK: { // Slack integration settings.
    MENTION_USER: "<@U08TUF8LW2H>" // The Slack user ID to mention in notifications.
  }
};

/**
 * --- SCRIPT INITIATION ---
 * This is the main entry point to start the entire validation process.
 * It performs initial checks, sets up the environment, and starts the first batch.
 */
function AHA_StartValidation2() {
  const start = new Date(); // Record the start time for performance logging.
  try {
    // Notify Slack that the process is starting.
    AHA_SlackNotify3("Category Importing is " + CONFIG.CATEGORY_IMPORTING_ENABLED);

    // Conditionally run setup functions based on the feature flag.
    if (!CONFIG.CATEGORY_IMPORTING_ENABLED) {
      AHA_TempSheetBAProdukSHO2("BA Produk SHO"); // USE ONLY WHEN BROKEN
    } else {
      AHA_StartCreateTempSheetsBatch2(); // Standard setup for creating temp sheets.
    }

    // --- SAFETY CHECK (CIRCUIT BREAKER) ---
    // Check for a critical error state before proceeding.
    if (AHA_CekBrandValidation2()) {
      AHA_SlackNotify3(`⚠️ Brand Validation sedang Error. Contact team FBI. Skipping Batch for 5 Minutes ${CONFIG.SLACK.MENTION_USER}`);
      // Create a trigger to re-run this entire start function after a 5-minute cooldown.
      ScriptApp.newTrigger("AHA_StartValidation2")
        .timeBased()
        .after(5 * 60 * 1000) // 5 minutes in milliseconds.
        .create();
      return; // Stop the current execution to wait for the trigger.
    }

    // --- ENVIRONMENT SETUP ---
    const moveFolder = AHA_GetMoveFolder2(); // Get the specific subfolder for this run.
    if (!moveFolder) {
        throw new Error("Could not get or create the move folder. Halting execution.");
    }
    const moveFolderId = moveFolder.getId();
    
    // Prepare the "Input" sheet for the new validation run.
    const inputSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(CONFIG.SHEET_NAMES.INPUT);
    // inputSheet.getRange("A1").setValue("VALIDATING"); // Set the master status.
    inputSheet.getRange("B5:F").clearContent(); // Clear previous results.
    inputSheet.getRange("B3").setValue(5); // Reset the row tracker.
    
    // Store the processing folder's ID for other functions to access.
    PropertiesService.getScriptProperties().setProperty("MOVE_FOLDER_ID", moveFolderId);

    AHA_UpdateCellUsage2(); // Update cell usage statistics.

    // Start the first batch processing run immediately.
    AHA_RunValBatchSafely2();
  } catch (err) {
    // --- NEW: CATCH CRITICAL STARTUP ERRORS ---
    const msg = `❌ *CRITICAL SYSTEM FAILURE*: Validation could not start! \n*Reason*: ${err.message} \n${CONFIG.SLACK.MENTION_USER}`;
    console.error(msg);
    AHA_SlackNotify3(msg);
  } finally {
    // This block runs whether the function succeeds or fails.
    const end = new Date();
    AHA_LogRuntime3(end - start); // Log the total execution time.
  }
}

/**
 * Helper function to check for a critical error state in the Input sheet.
 * -- MODIFIED to get the validation sheet name at runtime --
 * @returns {boolean} True if the error message is present, false otherwise.
 */
function AHA_CekBrandValidation2() {
  const start = new Date();
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    
    // --- FIX: Get the category and sheet name at runtime ---
    const category = PropertiesService.getScriptProperties().getProperty("WORKER_CATEGORY");
    if (!category) {
      // This can happen if setup hasn't run. Fail safely.
      AHA_SlackNotify3("⚠️ AHA_CekBrandValidation2: WORKER_CATEGORY is not set. Assuming no error.");
      return false; 
    }
    
    const sheetName = category + " Validation";
    const inputSheet = ss.getSheetByName(sheetName);
    
    if (!inputSheet) {
      // If the validation sheet doesn't exist, that's not a "data" error,
      // so we let the process continue. The error will be caught later.
      Logger.log(`AHA_CekBrandValidation2: Sheet "${sheetName}" not found. Assuming no error.`);
      return false;
    }
    // --- END FIX ---

    const statusCell = inputSheet.getRange("A3"); // The specific cell that acts as an error flag.

    // Return true if the cell's value indicates an error.
    return statusCell.getValue() === "";

  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}

/**
 * Determines the category of a file by finding the BEST percentage-based match for its headers,
 * then validates it using a filename pattern if one is provided.
 * It uses rules defined in the "Type Validation" sheet.
 * @param {Array<any>} potentialHeaderRow The content of a row from the file that might be a header.
 * @param {string} fileName The name of the file being processed.
 * @returns {{category: string, headerRowIndex: number, dataRowIndex: number}} An object with the detected category and row indices.
 */
function AHA_GetCategory2(potentialHeaderRow, fileName) {
  const start = new Date();
  try {
  // --- Define the matching threshold. 0.8 means 80%. ---
    const MATCH_THRESHOLD = 0.8;

    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = ss.getSheetByName(CONFIG.SHEET_NAMES.TYPE_VALIDATION);

    if (!sheet) {
      AHA_SlackNotify3(`❌ *Error*: '${CONFIG.SHEET_NAMES.TYPE_VALIDATION}' sheet not found. ${CONFIG.SLACK.MENTION_USER}`);
      throw new Error(`Sheet '${CONFIG.SHEET_NAMES.TYPE_VALIDATION}' not found`);
    }

    const lastRow = sheet.getLastRow();
    const lastCol = sheet.getLastColumn();

    if (lastRow < 2) {
      return { category: "Unknown", headerRowIndex: 1, dataRowIndex: 2 };
    }

    const validationData = sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();

    if (!Array.isArray(potentialHeaderRow)) {
      return { category: "Unknown", headerRowIndex: 1, dataRowIndex: 2 };
    }
    const lowerPotentialHeaderRow = potentialHeaderRow.map(cell => (cell || "").toString().toLowerCase());
    const lowerFileName = fileName.toLowerCase();

    // --- MODIFIED LOGIC: Two-Phase Matching (Score then Filter) ---
    let candidateMatches = [];

    // 1. SCORING PHASE: Loop through ALL categories to find potential candidates above the threshold.
    for (let i = 0; i < validationData.length; i++) {
      const cat = validationData[i][0];
      const headerRowIdx = validationData[i][1];
      const dataRowIdx = validationData[i][2];
      const fileNamePattern = validationData[i][3];
      const requiredHeaderKeys = validationData[i].slice(4)
        .filter(key => key && key.toString().trim() !== '')
        .map(key => key.toString().toLowerCase());

      if (requiredHeaderKeys.length === 0) {
        continue;
      }

      const matchingKeys = requiredHeaderKeys.filter(requiredKey =>
        lowerPotentialHeaderRow.includes(requiredKey)
      );
      const currentScore = matchingKeys.length / requiredHeaderKeys.length;

      // Only consider categories that meet the threshold as potential candidates.
      if (currentScore >= MATCH_THRESHOLD) {
        const currentHeaderRowIndex = (typeof headerRowIdx === 'number' && !isNaN(headerRowIdx) && headerRowIdx >= 1) ? headerRowIdx : 1;
        const currentDataRowIndex = (typeof dataRowIdx === 'number' && !isNaN(dataRowIdx) && dataRowIdx >= 1) ? dataRowIdx : (currentHeaderRowIndex + 1);
        
        candidateMatches.push({
          category: cat,
          score: currentScore,
          headerRowIndex: currentHeaderRowIndex,
          dataRowIndex: currentDataRowIndex,
          fileNamePattern: (fileNamePattern || "").toString().toLowerCase()
        });
      }
    }

    // If there are no candidates after the first phase, we're done.
    if (candidateMatches.length === 0) {
      Logger.log(`No category met the ${MATCH_THRESHOLD*100}% threshold. Returning 'Unknown'.`);
      return { category: "Unknown", headerRowIndex: 1, dataRowIndex: 2 };
    }
    
    // Sort candidates by score, highest first, to ensure we check the best header match first.
    candidateMatches.sort((a, b) => b.score - a.score);

    // 2. FILTERING PHASE: Find the best valid match from the candidates.
    for (const match of candidateMatches) {
      // If a filename pattern is required for this high-scoring category...
      if (match.fileNamePattern) {
      // ...the file's name MUST match it.
        if (lowerFileName.includes(match.fileNamePattern)) {
          Logger.log(`✅ Best match '${match.category}' (${(match.score*100).toFixed(0)}%) also matches filename pattern.`);
          return match; // This is our winner.
        } else {
          Logger.log(`⚠️ Candidate '${match.category}' (${(match.score*100).toFixed(0)}%) was rejected due to filename mismatch.`);
          continue; // Disqualify this one and check the next best candidate.
        }
      } else {
        // If no filename pattern is required, the highest score wins.
        Logger.log(`✅ Best match is '${match.category}' (${(match.score*100).toFixed(0)}%) with no filename pattern required.`);
        return match; // This is our winner.
      }
    }

    // If all high-scoring candidates were disqualified (e.g., by filename mismatch), return Unknown.
    Logger.log(`No suitable category found after filtering. Returning 'Unknown'.`);
    return { category: "Unknown", headerRowIndex: 1, dataRowIndex: 2 };

  } catch (error) {
    Logger.log(`Error in AHA_GetCategory2: ${error.message}`);
    AHA_SlackNotify3(`❌ *Error in GetCategory2*: ${error.message} ${CONFIG.SLACK.MENTION_USER}`);
    throw error;
  } finally {
    const end = new Date();
    // AHA_LogRuntime3(end - start);
  }
}

/**
 * --- VALIDATION DISPATCHER ---
 * Routes a file to the correct validation logic based on its category.
 */
function AHA_ValidateData2(category, data, file, detectedCategoryInfo = null) {
    try {
        // --- Handle all BA Dash files with a special path ---
        if (category.includes("BA Dash")) {
            const folderName = AHA_MoveFile2(file, "DERIVE_FROM_FILENAME", category, data);
            if (folderName === "Move Error" || folderName === "Failed") {
                return ["Wrong Data", "Failed"];
            } else {
                return ["Validated", folderName];
            }
        }

        // --- Handle auto-validated categories ---
        const autoValidatedCategories = ["Demografis BSL"];
        if (autoValidatedCategories.includes(category)) {
            const moveResultFolder = AHA_MoveFile2(file, "Validated", category, data);
            return ["Validated", moveResultFolder];
        }

        // --- Handle cell-content validation categories ---
        const validationRules = {};
        const cellValidationCategories = [
            "BA Produk LAZ", "BA Produk TIK", "BA Produk SHO",
            "Informasi Dasar SHO", "Informasi Penjualan SHO", "Informasi Media SHO",
            "Informasi Dikirim Dalam SHO", "Export SKU LAZ", "Export SKU TIK"
        ];
        cellValidationCategories.forEach(cat => {
            validationRules[cat] = { validationSheet: `${cat} Validation` };
        });

        const rule = validationRules[category];
        if (rule) {
            const result = AHA_MoveFile2(file, rule.validationSheet, category, data, detectedCategoryInfo);
            if (result === "Move Error" || result === "Failed") {
                return ["Wrong Data", "Failed"];
            } else {
                return ["Validated", result];
            }
        } else {
            const result = AHA_MoveFile2(file, "Failed", category, data);
            return ["Unknown Format", result];
        }
    } catch (err) {
        AHA_LogAndNotify(`Validation failed for ${file.getName()}: ${err.message}`, true);
        // --- MODIFICATION: Re-throw the error to trigger the main retry loop ---
        throw err;
    }
}

/**
 * --- CORE BATCH PROCESSING ---
 * Processes a batch of files: identifies category, validates, moves, and logs results.
 * -- MODIFIED to log failures to a central Google Doc. --
 */
function AHA_ValidationBatch2() {
    const start = new Date();
    try {
        const ss = SpreadsheetApp.getActiveSpreadsheet();
        const inputSheet = ss.getSheetByName(CONFIG.SHEET_NAMES.INPUT);
        if (!inputSheet) throw new Error(`Sheet '${CONFIG.SHEET_NAMES.INPUT}' not found.`);

        const moveFolderId = PropertiesService.getScriptProperties().getProperty("MOVE_FOLDER_ID");
        const folder = DriveApp.getFolderById(moveFolderId);
        if (!folder) throw new Error(`Move folder not found by ID: ${moveFolderId}.`);

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

        const files = folder.getFiles();
        const rowProgressCell = inputSheet.getRange("B3");
        const startRow = Number(rowProgressCell.getValue());

        const results = [];
        let processedCount = 0;

        while (files.hasNext() && processedCount < CONFIG.BATCH_SIZE) {
            const file = files.next();
            const name = file.getName();
            let category = "Unknown";
            let validationResult = "Not Checked";
            let parentFolderName = "Move";
            let processingStatus = "Not Yet Added";

            const maxRetries = 3;
            const retryDelay = 10000; // 10 seconds

            for (let attempt = 0; attempt < maxRetries; attempt++) {
                try {
                    let guessedCategory = "Unknown";
                    const lowerName = name.toLowerCase();

                    if (lowerName.includes('.shopee-shop-stats.')) {
                        guessedCategory = "BA Dash SHO";
                    } else if (lowerName.includes('bisnis analisis - dashboard')) {
                        if (lowerName.startsWith('tik ')) { guessedCategory = "BA Dash TIK"; }
                        else if (lowerName.startsWith('tok ')) { guessedCategory = "BA Dash TOK"; }
                        else { guessedCategory = "BA Dash LAZ"; }
                    }
                    
                    if (guessedCategory !== "Unknown") {
                        [validationResult, parentFolderName] = AHA_ValidateData2(guessedCategory, null, file);
                        category = guessedCategory;
                    }
                    
                    if (validationResult === "Not Checked") {
                        const blob = file.getBlob();
                        const resource = { title: name, mimeType: MimeType.GOOGLE_SHEETS, parents: [{ id: moveFolderId }] };
                        const newFile = Drive.Files.insert(resource, blob, { convert: true, supportsAllDrives: true });
                        const newFileId = newFile.id;
                        try {
                            const tempSheet = SpreadsheetApp.openById(newFileId).getSheets()[0];
                            const initialData = tempSheet.getRange(1, 1, Math.min(tempSheet.getLastRow(), 15), tempSheet.getLastColumn()).getValues();
                            let detectedCategoryInfo = { category: "Unknown", headerRowIndex: 1, dataRowIndex: 2 };
                            for (const rowIndexToCheck of potentialHeaderRowsToCheck) {
                                if (initialData[rowIndexToCheck] && Array.isArray(initialData[rowIndexToCheck])) {
                                    const result = AHA_GetCategory2(initialData[rowIndexToCheck], name);
                                    if (result.category !== "Unknown") {
                                        detectedCategoryInfo = result;
                                        break;
                                    }
                                }
                            }
                            category = detectedCategoryInfo.category;
                            const fullData = tempSheet.getDataRange().getValues();
                            [validationResult, parentFolderName] = AHA_ValidateData2(category, fullData, file, detectedCategoryInfo);
                        } finally {
                            if (newFileId) Drive.Files.remove(newFileId, { supportsAllDrives: true });
                        }
                    }
                    
                    if (validationResult === "Wrong Data" || validationResult === "Move Error" || validationResult === "Not Valid") {
                        processingStatus = "Completed";
                    }

                    break; // Success! Exit the retry loop.

                } catch (error) {
                    Logger.log(`Error processing file ${name} on attempt ${attempt + 1}/${maxRetries}: ${error.toString()}`);
                    
                    if (attempt < maxRetries - 1) {
                        Utilities.sleep(retryDelay);
                    } else {
                        // --- THIS IS THE FINAL FAILURE BLOCK ---
                        AHA_SlackNotify3(`${CONFIG.SLACK.MENTION_USER} ❌ Error processing file ${name} after ${maxRetries} attempts: ${error.toString()}`);
                        processingStatus = "Failed"; 
                        validationResult = "Process Error";
                        parentFolderName = AHA_MoveFile2(file, "Failed", category, null);

                        // --- NEW LINE: Log this failure to the Google Doc ---
                        const workerName = PropertiesService.getScriptProperties().getProperty("WORKER_COUNT") || "Unknown Worker";
                        AHA_LogFailureToDoc(name, `Process Error: ${error.message}`, category, workerName);
                    }
                }
            } // --- End of retry loop ---

            // --- ALSO LOG "WRONG DATA" FAILURES ---
            if (validationResult === "Wrong Data") {
                 const workerName = PropertiesService.getScriptProperties().getProperty("WORKER_COUNT") || "Unknown Worker";
                 AHA_LogFailureToDoc(name, validationResult, category, workerName);
            }

            const lower = name.toLowerCase();
            if (lower.endsWith(".xlsx") || lower.endsWith(".xls")) {
                results.push([parentFolderName, name, processingStatus, category, validationResult]);
                AHA_SlackNotify3(`Validated File: ${parentFolderName}, ${name}, ${processingStatus}, ${category}, ${validationResult}`);
            }

            processedCount++;
        }

        if (results.length > 0) {
            inputSheet.getRange(startRow, 2, results.length, 5).setValues(results);
            rowProgressCell.setValue(startRow + results.length);
        }

        if (folder.getFiles().hasNext()) {
            AHA_StartValTrigger2(1);
        } else {
            PropertiesService.getScriptProperties().deleteProperty("MOVE_FOLDER_ID");
            AHA_RemoveValTriggers2("AHA_RunValBatchSafely2");
            AHA_SlackNotify3("✅ Validation Completed. Starting Import...");
            AHA_StartImport2();
        }

        PropertiesService.getScriptProperties().setProperty("LAST_VALIDATION_HEARTBEAT", new Date().getTime());

    } finally {
        const end = new Date();
        AHA_LogRuntime3(end - start);
    }
}

/**
 * Creates a time-based trigger to run the next validation batch.
 * @param {number} minut The number of minutes to wait before the trigger runs.
 */
function AHA_StartValTrigger2(minut = 1) {
  const start = new Date();
  try {
    // Remove old triggers to prevent duplicates.
    AHA_RemoveValTriggers2("AHA_RunValBatchSafely2");
    // Create a new trigger.
    ScriptApp.newTrigger("AHA_RunValBatchSafely2")
      .timeBased()
      .after(minut * 60 * 1000)
      .create();
  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}

/**
 * A safe wrapper for the batch processor, using a lock to prevent concurrent executions.
 * -- MODIFIED to update the heartbeat on lock failure to prevent false "stale" state. --
 */
function AHA_RunValBatchSafely2() {
  const start = new Date();
  try {
    const lock = LockService.getScriptLock();
    if (!lock.tryLock(10000)) {
      Logger.log("Another instance is already running.");
      AHA_SlackNotify3("⚠️ Trigger cancelled: Another instance is already running...");
      
      // --- THE FIX ---
      // Update the heartbeat anyway. This tells the Watchdog "I'm alive, just locked."
      // This prevents the Watchdog from seeing a "stale" process and entering a quarantine loop.
      PropertiesService.getScriptProperties().setProperty("LAST_VALIDATION_HEARTBEAT", new Date().getTime());
      // --- END FIX ---

      return; // Exit if another instance has the lock.
    }

    try {
      if (AHA_CekBrandValidation2()) {
        const logSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(CONFIG.SHEET_NAMES.LOGS);
        Logger.log("Brand Validation sedang Error. Skipping Batch ~");
        logSheet.appendRow([new Date(), "Status", "⚠️ Brand Validation sedang Error. Contact tim FBI. Skipping Batch ~"]);
        AHA_SlackNotify3(`⚠️ Brand Validation sedang Error. Contact team FBI. Skipping Batch for 5 Minutes ${CONFIG.SLACK.MENTION_USER}`);
        AHA_StartValTrigger2(5); // Reschedule for later.
        return;
      } else {
        AHA_ValidationBatch2();
      }
    } catch (err) {
      const logSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(CONFIG.SHEET_NAMES.LOGS);
      Logger.log(`Error in runValBatchSafely: ${err.toString()}`);
      logSheet.appendRow([new Date(), "Error", err.toString()]);
      AHA_SlackNotify3(`❌ Error in runValBatchSafely: ${err.toString()} ${CONFIG.SLACK.MENTION_USER}`);

      // --- THE FIX (PART 2) ---
      // Also update the heartbeat on a crash, just in case.
      PropertiesService.getScriptProperties().setProperty("LAST_VALIDATION_HEARTBEAT", new Date().getTime());
      // --- END FIX ---
      
    } finally {
      lock.releaseLock();
    }
  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}


/**
 * --- FILE MOVER & VALIDATOR ---
 * Performs the actual validation and moves the file to the appropriate folder.
 */
function AHA_MoveFile2(file, targetFolderIdentifier, category = null, sheetData = null) {
    try {
        const ss = SpreadsheetApp.getActiveSpreadsheet();
        const parentFolder = DriveApp.getFolderById(CONFIG.FOLDER_IDS.ROOT_SHARED_DRIVE);
        if (!parentFolder) {
            throw new Error(`Parent Drive folder not found by ID: ${CONFIG.FOLDER_IDS.ROOT_SHARED_DRIVE}.`);
        }

        let actualTargetFolderName = "Failed";

        if (targetFolderIdentifier === "DERIVE_FROM_FILENAME") {
            if (category === "BA Dash SHO") {
                const shopName = file.getName().split(".shopee-shop-stats.")[0].trim();
                if (shopName) {
                    const validationSheet = ss.getSheetByName("BA Dash SHO Validation");
                    if (!validationSheet) {
                        throw new Error("Validation Sheet 'BA Dash SHO Validation' not found. It is required for this category.");
                    }
                    const mappingData = validationSheet.getRange("B1:C" + validationSheet.getLastRow()).getValues();
                    for (const row of mappingData) {
                        const folderName = row[0];
                        const validationValue = row[1];
                        if (validationValue && validationValue.toString().trim() === shopName) {
                            actualTargetFolderName = folderName.toString();
                            break;
                        }
                    }
                }
            } else {
                let valueToCheck = null;
                switch (category) {
                    case "BA Dash TIK":
                    case "BA Dash TOK":
                        valueToCheck = file.getName().split(' ')[1] || null;
                        break;
                    case "BA Dash LAZ":
                        valueToCheck = file.getName().split(' ')[0] || null;
                        break;
                }

                // --- NEW VALIDATION LOGIC FOR BA DASH TIK, TOK, and LAZ ---
                if (["BA Dash TIK", "BA Dash TOK", "BA Dash LAZ"].includes(category)) {
                    if (valueToCheck && AHA_ValidateBrandCodeFromSheet(valueToCheck, category)) {
                        // Brand code is valid, proceed as normal.
                        actualTargetFolderName = valueToCheck;
                    } else {
                        // Brand code is NOT valid, trigger failure.
                        AHA_SlackNotify3(`❌ *Invalid Filename*: Brand code '${valueToCheck || "NULL"}' in file '${file.getName()}' is not valid for category '${category}'. Moving to Failed. ${CONFIG.SLACK.MENTION_USER}`);
                        actualTargetFolderName = "Failed";
                    }
                } else if (valueToCheck) {
                    // Fallback for any other categories that might use this logic.
                    actualTargetFolderName = valueToCheck;
                }
                // --- END OF NEW LOGIC ---
            }
        } else if (targetFolderIdentifier === "Failed" || targetFolderIdentifier === "Validated") {
            actualTargetFolderName = targetFolderIdentifier;
        } else {
            const validationSheet = ss.getSheetByName(targetFolderIdentifier);
            if (!validationSheet) {
                throw new Error(`Validation Sheet '${targetFolderIdentifier}' not found.`);
            }
            if (!sheetData) {
                throw new Error(`Missing sheetData for cell-based validation of category '${category}'.`);
            }
            
            const typeValidationSheet = ss.getSheetByName(CONFIG.SHEET_NAMES.TYPE_VALIDATION);
            if (!typeValidationSheet) throw new Error("Sheet 'Type Validation' not found.");

            const categoriesInTypeSheet = typeValidationSheet.getRange("A2:A").getValues().flat();
            const categoryRowIndex = categoriesInTypeSheet.indexOf(category);
            let dataRowNumber = -1;

            if (categoryRowIndex !== -1) {
                dataRowNumber = typeValidationSheet.getRange(categoryRowIndex + 2, 3).getValue();
            }

            if (typeof dataRowNumber !== 'number' || dataRowNumber < 1) {
                AHA_SlackNotify3(`⚠️ Invalid Data Row Index for category '${category}'. Moving to 'Failed'.`);
            } else {
                const startRowIndex = dataRowNumber - 1;
                const NUM_ROWS_TO_CHECK = 20;
                const MATCH_THRESHOLD_PERCENT = 0.50;
                const endRowIndex = Math.min(startRowIndex + NUM_ROWS_TO_CHECK, sheetData.length);
                const valuesToCheck = sheetData
                    .slice(startRowIndex, endRowIndex)
                    .map(row => row && row[0])
                    .filter(value => value !== null && value !== undefined && value.toString().trim() !== '');

                if (valuesToCheck.length > 0) {
                    const mapping = validationSheet.getRange("A2:B").getValues();
                    const validationMap = new Map(
                        mapping.map(row => [row[1]?.toString().trim(), row[0]?.toString().trim()]).filter(([id, folder]) => id && folder)
                    );
                    let matchCount = 0;
                    const folderNameCounts = {};
                    for (const value of valuesToCheck) {
                        const trimmedValue = value.toString().trim();
                        if (validationMap.has(trimmedValue)) {
                            matchCount++;
                            const folderName = validationMap.get(trimmedValue);
                            folderNameCounts[folderName] = (folderNameCounts[folderName] || 0) + 1;
                        }
                    }
                    const requiredMatches = valuesToCheck.length * MATCH_THRESHOLD_PERCENT;
                    if (matchCount >= requiredMatches && matchCount > 0) {
                        actualTargetFolderName = Object.keys(folderNameCounts).reduce((a, b) => folderNameCounts[a] > folderNameCounts[b] ? a : b);
                    }
                }
            }
        }

        const folders = parentFolder.getFoldersByName(actualTargetFolderName);
        const targetFolder = folders.hasNext() ? folders.next() : parentFolder.createFolder(actualTargetFolderName);
        
        Drive.Files.update({ parents: [{ id: targetFolder.getId() }] }, file.getId(), null, { supportsAllDrives: true });
        DriveApp.getFolderById(PropertiesService.getScriptProperties().getProperty("MOVE_FOLDER_ID")).removeFile(file);

        return actualTargetFolderName;

    } catch (err) {
        Logger.log(`Move error for file ${file.getName()}: ${err.toString()}`);
        // --- MODIFICATION: Re-throw the error to trigger the retry loop ---
        // This will allow the general retry mechanism in AHA_ValidationBatch2 to catch it.
        throw err;
    }
}

/**
 * Gets or creates a worker-specific subfolder for processing files.
 * @returns {GoogleAppsScript.Drive.Folder} The target folder object.
 */
function AHA_GetMoveFolder2() {
  const start = new Date();
  try {
    const assignedFolderName = PropertiesService.getScriptProperties().getProperty("WORKER_COUNT");
    const categoryName = PropertiesService.getScriptProperties().getProperty("WORKER_CATEGORY");
    const baseMoveFolderId = CONFIG.FOLDER_IDS.MOVE_FOLDER;

    if (!assignedFolderName || !categoryName) {
        throw new Error("Worker properties (WORKER_COUNT, WORKER_CATEGORY) are not set.");
    }
    
    const baseMoveFolder = DriveApp.getFolderById(baseMoveFolderId);

    // Step 1: Get or create the folder for the file category.
    const categoryFolders = baseMoveFolder.getFoldersByName(categoryName);
    const categoryFolder = categoryFolders.hasNext() ?
      categoryFolders.next() :
      baseMoveFolder.createFolder(categoryName);

    // Step 2: Get or create the final folder for this specific worker.
    const assignedFolders = categoryFolder.getFoldersByName(assignedFolderName);
    const targetFolder = assignedFolders.hasNext() ?
      assignedFolders.next() :
      categoryFolder.createFolder(assignedFolderName);
      
    Logger.log(`Ensured 'Move' folder exists: ${targetFolder.getName()} (${targetFolder.getId()})`);
    return targetFolder;

  } catch (err) {
      const errorMessage = `❌ Critical error in AHA_GetMoveFolder2: ${err.message}`;
      AHA_SlackNotify3(`${errorMessage} ${CONFIG.SLACK.MENTION_USER}`);
      throw new Error(errorMessage);
  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}

/**
 * Helper function to remove all triggers for a specific function name.
 * @param {string} functionName The name of the function whose triggers should be deleted.
 */
function AHA_RemoveValTriggers2(functionName) {
  const start = new Date();
  try {
    const allTriggers = ScriptApp.getProjectTriggers();
    allTriggers.forEach(trigger => {
      if (trigger.getHandlerFunction() === functionName) {
        ScriptApp.deleteTrigger(trigger);
      }
    });
  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}


/**
 * Checks if a given brand code exists in Column B of the category's validation sheet.
 * @param {string} brandCode The brand code extracted from the filename.
 * @param {string} category The category being validated (e.g., "BA Dash TIK").
 * @returns {boolean} True if the brand code is valid, false otherwise.
 */
function AHA_ValidateBrandCodeFromSheet(brandCode, category) {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const validationSheetName = `${category} Validation`;
    const sheet = ss.getSheetByName(validationSheetName);

    if (!sheet) {
      AHA_SlackNotify3(`⚠️ *Missing Sheet*: Validation sheet named '${validationSheetName}' not found. Cannot validate brand code '${brandCode}'.`);
      Logger.log(`Validation sheet not found: ${validationSheetName}`);
      return false; // Fail safely if the sheet doesn't exist.
    }

    const lastRow = sheet.getLastRow();
    if (lastRow < 2) {
      return false; // No data to validate against.
    }

    // Get all brand codes from Column B and create a Set for fast lookup.
    const validBrandCodes = sheet.getRange(`B2:B${lastRow}`).getValues().flat().filter(String);
    const validBrandCodeSet = new Set(validBrandCodes);

    return validBrandCodeSet.has(brandCode);

  } catch (err) {
    Logger.log(`Error in AHA_ValidateBrandCodeFromSheet for brand '${brandCode}': ${err.message}`);
    AHA_SlackNotify3(`❌ *Error*: An error occurred during brand code validation for '${brandCode}'. ${CONFIG.SLACK.MENTION_USER}`);
    return false;
  }
}

// [File: Phase 2 Validation.js]

/**
 * Appends a new failure log entry to the central Google Sheet.
 * This version uses SpreadsheetApp.appendRow() and requires no new permissions.
 *
 * @param {string} fileName The name of the file that failed.
 * @param {string} reason The reason for the failure (e.g., "Wrong Data", "Process Error").
 * @param {string} category The category that was processing the file.
 * @param {string} workerName The name of the worker (e.g., "Worker 1").
 */
function AHA_LogFailureToDoc(fileName, reason, category, workerName) {
  try {
    if (!CONFIG.FAILURE_LOG_DOC_ID) { // This variable now holds the Sheet ID
      Logger.log("FAILURE_LOG_DOC_ID (Sheet ID) not set in CONFIG. Skipping log.");
      return;
    }

    // Format the log row
    const timestamp = new Date(); // Use a date object, Google Sheets prefers this
    const workerInfo = `${category} - ${workerName}`;

    // This is the data to append. Make sure your log sheet has 4 columns:
    // Timestamp | File Name | Reason | Worker Info
    const logLine = [timestamp, fileName, reason, workerInfo];

    AHA_ExecuteWithRetry(() => {
      // Open the central LOG SPREADSHEET
      const logSpreadsheet = SpreadsheetApp.openById(CONFIG.FAILURE_LOG_DOC_ID);

      // Get the specific sheet (e.g., "Failures" or just the first sheet)
      const logSheet = logSpreadsheet.getSheets()[0]; // Or use .getSheetByName("Failures")

      // Append the data as a new row
      logSheet.appendRow(logLine);

    }, 'LogFailureToSheet', 3, 2000); // Retry 3 times

  } catch (err) {
    Logger.log(`CRITICAL: Failed to write to Failure Log Sheet after all retries: ${err.message}`);
    AHA_SlackNotify3(`❌ CRITICAL: Failed to write to Failure Log Sheet for file ${fileName}. ${CONFIG.SLACK.MENTION_USER}`);
  }
}










