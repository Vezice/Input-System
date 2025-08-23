////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Validation.gs 
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// mass edit confirmed

// --- CONFIGURATION OBJECT ---
// Central place to store all settings and magic strings for easy management.
const CONFIG = {
  BATCH_SIZE: 10, // The number of files to process in a single execution run to avoid timeouts.
  CATEGORY_IMPORTING_ENABLED: true, // A feature flag to enable or disable category importing.
  SHEET_NAMES: { // Standardizes sheet names used throughout the script.
    INPUT: "Input",
    LOGS: "Logs",
    TYPE_VALIDATION: "Type Validation"
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
    inputSheet.getRange("A1").setValue("VALIDATING"); // Set the master status.
    inputSheet.getRange("B5:F").clearContent(); // Clear previous results.
    inputSheet.getRange("B3").setValue(5); // Reset the row tracker.
    
    // Store the processing folder's ID for other functions to access.
    PropertiesService.getScriptProperties().setProperty("MOVE_FOLDER_ID", moveFolderId);

    AHA_UpdateCellUsage2(); // Update cell usage statistics.

    // Start the first batch processing run immediately.
    AHA_RunValBatchSafely2();

  } finally {
    // This block runs whether the function succeeds or fails.
    const end = new Date();
    AHA_LogRuntime3(end - start); // Log the total execution time.
  }
}

/**
 * Helper function to check for a critical error state in the Input sheet.
 * @returns {boolean} True if the error message is present, false otherwise.
 */
function AHA_CekBrandValidation2() {
  const start = new Date();
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const inputSheet = ss.getSheetByName(CONFIG.SHEET_NAMES.INPUT);
    const statusCell = inputSheet.getRange("E1"); // The specific cell that acts as an error flag.

    // Return true if the cell's value indicates an error.
    return statusCell.getValue() === "Brand Validation Error!!";

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
 * --- CORE BATCH PROCESSING ---
 * Processes a batch of files: identifies category, validates, moves, and logs results.
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

    // --- OPTIMIZATION: Pre-fetch potential header row numbers to avoid reading sheet inside loop ---
    const headerRowIndicesFromSheet = typeValidationSheet.getRange(2, 2, typeValidationSheet.getLastRow() - 1, 1).getValues()
      .flat()
      .filter(rowIdx => typeof rowIdx === 'number' && rowIdx >= 1)
      .map(rowIdx => rowIdx - 1); // Convert to 0-based index.
    const potentialHeaderRowsToCheck = [...new Set(headerRowIndicesFromSheet)].sort((a, b) => a - b);
    if (potentialHeaderRowsToCheck.length === 0) {
      Logger.log(`⚠️ No valid 'Header Row Index' found. Defaulting to check row 1.`);
      potentialHeaderRowsToCheck.push(0);
    }
    // --- END OPTIMIZATION ---

    const files = folder.getFiles();
    const rowProgressCell = inputSheet.getRange("B3");
    const startRow = Number(rowProgressCell.getValue());

    const results = []; // Array to hold processing results for this batch.
    let processedCount = 0;

    // Loop through files up to the BATCH_SIZE limit.
    while (files.hasNext() && processedCount < CONFIG.BATCH_SIZE) {
      const file = files.next();
      const name = file.getName();
      let category = "Unknown";
      let validationResult = "Not Checked";
      let parentFolderName = "Move";

      try {
        // --- LAZY CONVERSION OPTIMIZATION (for filename-based validation) ---
        if (name.toLowerCase().includes('ba dash')) {
            Logger.log(`⚡ Optimized Path: Attempting filename-only validation for '${name}'.`);
            let guessedCategory = "Unknown";
            if (name.includes('TIK')) guessedCategory = "BA Dash TIK";
            else if (name.includes('TOK')) guessedCategory = "BA Dash TOK";
            else if (name.includes('LAZ')) guessedCategory = "BA Dash LAZ";
            else if (name.includes('SHO')) guessedCategory = "BA Dash SHO";

            if (guessedCategory !== "Unknown") {
              // Call validation with `null` data to force filename logic.
              [validationResult, parentFolderName] = AHA_ValidateData2(guessedCategory, null, file);
              category = guessedCategory;
            }
        }
        
        // --- FULL CONTENT VALIDATION PATH ---
        if (validationResult === "Not Checked") {
            Logger.log(`Content Path: Converting '${name}' for full validation.`);
            const blob = file.getBlob();
            // Prepare request to convert the file (e.g., XLSX) to a Google Sheet.
            const resource = {
              title: name,
              mimeType: MimeType.GOOGLE_SHEETS,
              parents: [{ id: moveFolderId }]
            };

            // Use the advanced Drive API for conversion.
            const newFile = Drive.Files.insert(resource, blob, { convert: true, supportsAllDrives: true });
            const newFileId = newFile.id;

            try {
              // Open the temporary Google Sheet to read its content.
              const newFileMeta = DriveApp.getFileById(newFileId);
              if (newFileMeta.getMimeType() === MimeType.GOOGLE_SHEETS) {
                const tempSheet = SpreadsheetApp.openById(newFileId).getSheets()[0];
                const maxReadCols = Math.max(4, typeValidationSheet.getLastColumn());
                const maxRowToRead = potentialHeaderRowsToCheck.length > 0 ? Math.max(...potentialHeaderRowsToCheck) + 5 : 7;
                
                // Read the top part of the sheet to find the header.
                const initialData = tempSheet.getRange(1, 1, Math.min(tempSheet.getLastRow(), maxRowToRead), Math.min(tempSheet.getLastColumn(), maxReadCols)).getValues();
                let detectedCategoryInfo = { category: "Unknown", headerRowIndex: 1, dataRowIndex: 2 };

                // Loop through potential header rows to identify the category.
                for (const rowIndexToCheck of potentialHeaderRowsToCheck) {
                  if (initialData[rowIndexToCheck] && Array.isArray(initialData[rowIndexToCheck])) {
                    const result = AHA_GetCategory2(initialData[rowIndexToCheck], name);
                    if (result.category !== "Unknown") {
                      detectedCategoryInfo = result;
                      Logger.log(`Category '${result.category}' detected at file row ${rowIndexToCheck + 1}.`);
                      break; // Stop searching once a category is found.
                    }
                  }
                }
                
                category = detectedCategoryInfo.category;
                const fullData = tempSheet.getDataRange().getValues();
                // Call validation with the full data.
                [validationResult, parentFolderName] = AHA_ValidateData2(category, fullData, file);
              }
            } finally {
                // CRITICAL: Always delete the temporary converted file.
                Drive.Files.remove(newFileId, { supportsAllDrives: true });
            }
        }

      } catch (error) {
        Logger.log(`Error processing file ${name}: ${error.toString()}`);
        AHA_SlackNotify3(`${CONFIG.SLACK.MENTION_USER} ❌ Error processing file ${name}: ${error.toString()}`);
        validationResult = "Convert Error";
        parentFolderName = AHA_MoveFile2(file, "Failed", category, null);
      }

      // Add the file's results to the array for bulk writing to the sheet.
      const lower = name.toLowerCase();
      if (lower.endsWith(".xlsx") || lower.endsWith(".xls")) {
        results.push([parentFolderName, name, "Not Yet Added", category, validationResult]);
        AHA_SlackNotify3(`Validated File: ${parentFolderName}, ${name}, Not Yet Added, ${category}, ${validationResult}`);
      }

      processedCount++;
    }

    // --- WRITE BATCH RESULTS & SCHEDULE NEXT RUN ---
    if (results.length > 0) {
      inputSheet.getRange(startRow, 2, results.length, 5).setValues(results);
      rowProgressCell.setValue(startRow + results.length);
    }

    // If more files exist, schedule the next batch.
    if (folder.getFiles().hasNext()) {
      AHA_StartValTrigger2(1);
    } else {
      // --- COMPLETION ---
      inputSheet.getRange("A1").setValue("SCRIPT OFFLINE");
      PropertiesService.getScriptProperties().deleteProperty("MOVE_FOLDER_ID");
      AHA_RemoveValTriggers2("AHA_RunValBatchSafely2");

      const uniqueCategories = [...new Set(results.map(row => row[3]).filter(Boolean).map(cat => cat.trim()))];

      Logger.log("🔍 Detected Categories: " + uniqueCategories.join(", "));
      AHA_SlackNotify3("🔍 Detected Categories: " + uniqueCategories.join(", "));
      AHA_SlackNotify3("✅ Validation Completed. Starting Import...");

      // Kick off the next stage of the workflow.
      AHA_StartImport2();
    }
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
 */
function AHA_RunValBatchSafely2() {
  const start = new Date();
  try {
    // Get a script-level lock.
    const lock = LockService.getScriptLock();
    // Try to acquire the lock, waiting up to 10 seconds.
    if (!lock.tryLock(10000)) {
      Logger.log("Another instance is already running.");
      AHA_SlackNotify3("⚠️ Trigger cancelled: Another instance is already running...");
      return; // Exit if another instance has the lock.
    }

    try {
      // Re-check the critical error flag before each batch.
      if (AHA_CekBrandValidation2()) {
        const logSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(CONFIG.SHEET_NAMES.LOGS);
        Logger.log("Brand Validation sedang Error. Skipping Batch ~");
        logSheet.appendRow([new Date(), "Status", "⚠️ Brand Validation sedang Error. Contact tim FBI. Skipping Batch ~"]);
        AHA_SlackNotify3(`⚠️ Brand Validation sedang Error. Contact team FBI. Skipping Batch for 5 Minutes ${CONFIG.SLACK.MENTION_USER}`);
        AHA_StartValTrigger2(5); // Reschedule for later.
        return;
      } else {
        // If okay, run the main batch function.
        AHA_ValidationBatch2();
      }
    } catch (err) {
      const logSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(CONFIG.SHEET_NAMES.LOGS);
      Logger.log(`Error in runValBatchSafely: ${err.toString()}`);
      logSheet.appendRow([new Date(), "Error", err.toString()]);
      AHA_SlackNotify3(`❌ Error in runValBatchSafely: ${err.toString()} ${CONFIG.SLACK.MENTION_USER}`);
    } finally {
      // CRITICAL: Always release the lock.
      lock.releaseLock();
    }
  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}

/**
 * --- VALIDATION DISPATCHER ---
 * Routes a file to the correct validation logic based on its category.
 * @param {string} category The detected category of the file.
 * @param {Array<Array<any>>} data The file's content (can be null for filename-only validation).
 * @param {GoogleAppsScript.Drive.File} file The file object.
 * @returns {[string, string]} An array with the validation status and destination folder name.
 */
function AHA_ValidateData2(category, data, file) {
  const start = new Date();
  try {
    let validationStatus = "Unknown Format";
    let moveResultFolder = "Failed";

    try {
      // --- NEW LOGIC: List of categories to auto-validate without content checks ---
      const autoValidatedCategories = ["Demografis BSL"];

      if (autoValidatedCategories.includes(category)) {
        // If the category is in the special list, skip detailed validation.
        Logger.log(`✅ Auto-validating category '${category}'. No brand check needed.`);
        validationStatus = "Validated";
        // Move the file to a generic "Validated" folder.
        moveResultFolder = AHA_MoveFile2(file, "Validated", category, data);
        return [validationStatus, moveResultFolder];
      }
      // --- END NEW LOGIC ---

      // --- RULE DEFINITIONS ---
      // A map of categories to their corresponding validation sheet names.
      const validationRules = {
        // Filename-based validation categories.
        "BA Dash TIK": { validationSheet: "BA Dash TIK Validation" },
        "BA Dash TOK": { validationSheet: "BA Dash TOK Validation" },
        "BA Dash LAZ": { validationSheet: "BA Dash LAZ Validation" },
        "BA Dash SHO": { validationSheet: "BA Dash SHO Validation" }
      };

      // Categories that use cell content-based validation.
      const cellValidationCategories = [
        "BA Produk LAZ", "BA Produk TIK", "BA Produk SHO",
        "Informasi Dasar SHO", "Informasi Penjualan SHO", "Informasi Media SHO",
        "Informasi Dikirim Dalam SHO", "Export SKU LAZ", "Export SKU TIK"
        // Note: "Demografis BSL" is removed from this list as it's now handled above.
      ];

      // Dynamically add the cell-based validation rules.
      cellValidationCategories.forEach(cat => {
        validationRules[cat] = { validationSheet: `${cat} Validation` };
      });

      // Look up the rule for the given category.
      const rule = validationRules[category];

      if (rule) {
        // If a rule exists, call the move/validation function.
        const result = AHA_MoveFile2(file, rule.validationSheet, category, data);
        if (result === "Move Error" || result === "Failed") {
          validationStatus = (result === "Move Error") ? "Move Error" : "Wrong Data";
          moveResultFolder = "Failed";
        } else {
          validationStatus = "Validated";
          moveResultFolder = result;
        }
      } else {
        // If no rule exists, move the file to "Failed".
        const result = AHA_MoveFile2(file, "Failed", category, data);
        validationStatus = (category === "Unknown") ? "Not Valid" : "Unknown Format";
        moveResultFolder = result;
      }

      return [validationStatus, moveResultFolder];

    } catch (err) {
      Logger.log(`Validation failed: ${err}`);
      AHA_SlackNotify3(`⚠️ Validation failed: ${err} ${CONFIG.SLACK.MENTION_USER}`);
      const moveResult = AHA_MoveFile2(file, "Failed", category, data);
      return ["Validation Error", moveResult];
    }
  } finally {
    const end = new Date();
    // AHA_LogRuntime3(end - start);
  }
}

/**
 * --- FILE MOVER & VALIDATOR ---
 * Performs the actual validation and moves the file to the appropriate folder.
 * @param {GoogleAppsScript.Drive.File} file The file to move.
 * @param {string} targetFolderIdentifier The name of the validation sheet or a direct folder name like "Failed".
 * @param {string} category The detected category of the file.
 * @param {Array<Array<any>>} sheetData The file's content.
 * @returns {string} The name of the folder the file was moved to.
 */
function AHA_MoveFile2(file, targetFolderIdentifier, category = null, sheetData = null) {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const parentFolder = DriveApp.getFolderById(CONFIG.FOLDER_IDS.ROOT_SHARED_DRIVE);
    if (!parentFolder) {
      throw new Error(`Parent Drive folder not found by ID: ${CONFIG.FOLDER_IDS.ROOT_SHARED_DRIVE}.`);
    }

    let actualTargetFolderName = "Failed"; // Default to "Failed".

    // If the target is a simple folder name (like from the auto-validate logic), no checks are needed.
    if (targetFolderIdentifier === "Failed" || targetFolderIdentifier === "Validated") {
      actualTargetFolderName = targetFolderIdentifier;
    } else {
      // Otherwise, `targetFolderIdentifier` is a validation sheet name.
      const validationSheet = ss.getSheetByName(targetFolderIdentifier);
      if (!validationSheet) {
        throw new Error(`Validation Sheet '${targetFolderIdentifier}' not found.`);
      }

      const isFilenameValidation = category.includes("BA Dash");

      // --- FILENAME-BASED VALIDATION ---
      if (isFilenameValidation) {
        let valueToCheck = null; // The piece of the filename to validate.
        let mappingRange = "A2:B";

        // Extract the correct part of the filename based on category.
        switch (category) {
          case "BA Dash TIK":
          case "BA Dash TOK":
            valueToCheck = file.getName().split(' ')[1] || null;
            break;
          case "BA Dash LAZ":
            valueToCheck = file.getName().split(' ')[0] || null;
            break;
          case "BA Dash SHO":
            valueToCheck = file.getName().split(".shopee-shop-stats.")[0].trim();
            mappingRange = "C1:B200";
            break;
        }

        // If a value was extracted, find it in the validation sheet.
        if (valueToCheck) {
          const mapping = validationSheet.getRange(mappingRange).getValues();
          for (const row of mapping) {
            const folderName = (mappingRange === "C1:B200") ? row[0] : row[0];
            const validationValue = (mappingRange === "C1:B200") ? row[1] : row[1];
            if (validationValue && validationValue.toString().trim() === valueToCheck.trim()) {
              actualTargetFolderName = folderName.toString();
              break;
            }
          }
        }
        if (actualTargetFolderName === "Failed") {
          AHA_SlackNotify3(`⚠️ No match for filename value "${valueToCheck}" from category "${category}". Moving to 'Failed'.`);
        }

      // --- CELL CONTENT-BASED VALIDATION ---
      } else {
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
          // --- ROBUST MULTI-ROW VALIDATION ---
          const startRowIndex = dataRowNumber - 1;
          const NUM_ROWS_TO_CHECK = 20; // How many rows to sample.
          const MATCH_THRESHOLD_PERCENT = 0.50; // Required match percentage.

          const endRowIndex = Math.min(startRowIndex + NUM_ROWS_TO_CHECK, sheetData.length);
          // Get the values from the first column of the sampled data rows.
          const valuesToCheck = sheetData
            .slice(startRowIndex, endRowIndex)
            .map(row => row && row[0])
            .filter(value => value !== null && value !== undefined && value.toString().trim() !== '');

          if (valuesToCheck.length > 0) {
            // Create a Map for fast lookups (ID -> FolderName).
            const mapping = validationSheet.getRange("A2:B").getValues();
            const validationMap = new Map(
              mapping.map(row => [row[1]?.toString().trim(), row[0]?.toString().trim()]).filter(([id, folder]) => id && folder)
            );

            let matchCount = 0;
            const folderNameCounts = {}; // To count matches per folder.
            for (const value of valuesToCheck) {
              const trimmedValue = value.toString().trim();
              if (validationMap.has(trimmedValue)) {
                matchCount++;
                const folderName = validationMap.get(trimmedValue);
                folderNameCounts[folderName] = (folderNameCounts[folderName] || 0) + 1;
              }
            }

            // Check if the number of matches meets the threshold.
            const requiredMatches = valuesToCheck.length * MATCH_THRESHOLD_PERCENT;
            if (matchCount >= requiredMatches && matchCount > 0) {
              // Target folder is the one with the most matches.
              actualTargetFolderName = Object.keys(folderNameCounts).reduce((a, b) => folderNameCounts[a] > folderNameCounts[b] ? a : b);
              Logger.log(`✅ Validation success for '${category}': ${matchCount}/${valuesToCheck.length} matches. Target: ${actualTargetFolderName}`);
            } else {
              AHA_SlackNotify3(`⚠️ Validation failed for category "${category}". Found ${matchCount}/${valuesToCheck.length} matches. Moving to 'Failed'.`);
            }
          }
        }
      }
    }

    // --- FILE MOVE EXECUTION ---
    // Get the destination folder, or create it if it doesn't exist.
    const folders = parentFolder.getFoldersByName(actualTargetFolderName);
    const targetFolder = folders.hasNext() ? folders.next() : parentFolder.createFolder(actualTargetFolderName);
    
    // Use the advanced Drive API to move the file.
    Drive.Files.update({ parents: [{id: targetFolder.getId() }] }, file.getId(), null, { supportsAllDrives: true });
    // Explicitly remove the file from its original parent folder.
    DriveApp.getFolderById(PropertiesService.getScriptProperties().getProperty("MOVE_FOLDER_ID")).removeFile(file);

    return actualTargetFolderName;

  } catch (err) {
    Logger.log(`Move error for file ${file.getName()}: ${err.toString()}`);
    return "Move Error";
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