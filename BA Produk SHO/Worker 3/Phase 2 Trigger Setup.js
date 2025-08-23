////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Trigger Setup.gs 
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Synchronized

/**
 * --- STATUS CHECKER ---
 * Checks if there are any files left to import based on their status in the "Input" sheet.
 * @returns {boolean} Returns true if an import needs to run, otherwise false.
 */
function AHA_CekStatusImport2() {
  const start = new Date();
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const inputSheet = ss.getSheetByName("Input");

    const lastRow = inputSheet.getLastRow();
    // If there is no data, no need to run.
    if (lastRow < 5) {
      return false;
    }

    // Get all relevant data (Folder, -, Status) in one efficient call.
    const dataValues = inputSheet.getRange("B5:D" + lastRow).getValues();

    // Use .some() for performance. It stops searching as soon as it finds the first row
    // that meets the condition, rather than checking every single row.
    const needsImport = dataValues.some(row => {
      const folderName = row[0]; // Value from Column B
      const status = row[2];     // Value from Column D

      // The condition: A file needs to be imported if its status is "Not Yet Added"
      // AND it did not fail validation (folder name is not "Failed").
      return status === "Not Yet Added" && folderName !== "Failed";
    });

    return needsImport;

  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}

/**
 * --- SAFE BATCH RUNNER ---
 * This function is called by the time-based trigger. It uses a lock to ensure
 * only one instance of the import process runs at a time.
 * -- MODIFIED with a retry mechanism for acquiring the lock. --
 */
function AHA_RunImportBatchSafely2() {
    const start = new Date();
    try {
        const ss = SpreadsheetApp.getActiveSpreadsheet();
        const logSheet = ss.getSheetByName("Logs");
        const lock = LockService.getScriptLock();

        // --- NEW: Retry Logic Configuration ---
        const maxRetries = 3; // How many times to try getting the lock.
        const retryDelay = 2 * 60 * 1000; // 2 minutes in milliseconds.

        // --- NEW: Loop to attempt acquiring the lock ---
        let lockAcquired = false;
        for (let attempt = 0; attempt < maxRetries; attempt++) {
            if (lock.tryLock(10000)) {
                // Successfully got the lock
                lockAcquired = true;
                break; // Exit the retry loop
            }
            
            // If we're here, the lock was busy. Wait before the next attempt.
            if (attempt < maxRetries - 1) {
                Logger.log(`Lock is busy. Retrying in 2 minutes... (Attempt ${attempt + 1}/${maxRetries})`);
                Utilities.sleep(retryDelay);
            }
        }

        // If the lock was never acquired after all retries, give up.
        if (!lockAcquired) {
            Logger.log(`Could not acquire lock after ${maxRetries} attempts. Aborting.`);
            AHA_SlackNotify3("⚠️ *Trigger cancelled*: Could not acquire lock after multiple attempts.");
            return;
        }

        // If we get here, we have the lock. Proceed with the main logic.
        try {
            if (AHA_CekStatusImport2()) {
                AHA_ImportByCategoryBatch2();
            } else {
                Logger.log("No files left to process.");
                logSheet.appendRow([new Date(), "Status", "✅ All Files has been processed"]);
                AHA_SlackNotify3("✅ *Completed* : All Files has been processed!");
                AHA_RemoveAllTriggers2();
            }
        } catch (err) {
            Logger.log("Error in processNextBatch: " + err);
        } finally {
            // CRITICAL: Always release the lock when done.
            lock.releaseLock();
        }

    } finally {
        const end = new Date();
        AHA_LogRuntime3(end - start);
    }
}

/**
 * Helper function to install the time-based trigger that runs the import process.
 */
function AHA_InstallTrigger2() {
  const start = new Date();
  try {
    // Creates a new trigger that will call AHA_RunImportBatchSafely2 every 5 minutes.
    ScriptApp.newTrigger("AHA_RunImportBatchSafely2")
      .timeBased()
      .everyMinutes(5)
      .create();
  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}

/**
 * --- FINALIZATION & CLEANUP ---
 * This function is called when the import process is complete. It removes all triggers,
 * finalizes temp sheets, archives files, and sends final notifications.
 */
function AHA_RemoveAllTriggers2() {
  const start = new Date();
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const inputSheet = ss.getSheetByName("Input");

    // This section appears to be leftover code from an older workflow,
    // as the outputSheetName variable is not used by the new category-based system.
    const kode_mp = inputSheet.getRange("B1").getValues();
    const kode_akun = inputSheet.getRange("C1").getValues();
    const bulan = inputSheet.getRange("D1").getValues();
    const outputSheetName = kode_mp + " " + kode_akun + " " + bulan;

    // Set the master status to "OFFLINE".
    inputSheet.getRange("A1").setValues([["SCRIPT OFFLINE"]]);

    // Delete all existing triggers for this project to stop the script.
    const triggers = ScriptApp.getProjectTriggers();
    for (let t of triggers) {
      ScriptApp.deleteTrigger(t);
    }

    // --- Finalize Temp Sheets ---
    // This logic correctly chooses which finalization function to run based on the config flag.
    if (!CONFIG.CATEGORY_IMPORTING_ENABLED) {
      // If using the old system, finalize sheets one by one.
      const categoriesRange = inputSheet.getRange("E5:E" + inputSheet.getLastRow()).getValues();
      const allCategories = [...new Set(categoriesRange.flat().filter(Boolean))];
      allCategories.forEach(category => {
        try {
          AHA_FinalizeSheetSwap2(category);
        } catch (e) {
          Logger.log(`❌ Failed to finalize sheet swap for category ${category}: ${e.message}`);
          AHA_SlackNotify3("❌ *Error* : Failed to finalize sheet swap for category " + category + " : " + e.message + " <@U08TUF8LW2H>");
        }
      });
    } else {
      // If using the new category-based system, run the modern finalization function.
      AHA_FinalizeAllTempSheets2();
    }

    // --- Archive and Clean Up ---
    AHA_SlackNotify3("⚠️ *Archiving Files...*");
    AHA_ArchiveFilesByCategory2(); // Archive all processed files.
    AHA_DeleteUnusedTempSheets2(); // Delete leftover temp sheets like "Temp A", "Temp B", etc.
    AHA_SlackNotify3("✅ *Import Completed* ✅");

    // Set a one-time trigger for a final notification.
    ScriptApp.newTrigger('AHA_NotifyCentral3')
      .timeBased()
      .after(60 * 1000) // 1 minute later
      .create();

  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}

/**
 * --- SCRIPT INITIATION (IMPORT) ---
 * This is the entry point that kicks off the entire import sequence after validation is complete.
 */
function AHA_StartImport2() {
  const start = new Date();
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const inputSheet = ss.getSheetByName("Input");
    
    // Update the master status to "IMPORTING".
    inputSheet.getRange("A1").setValues([["IMPORTING"]]);
    AHA_SlackNotify3("⚠️ *Starting Import...*");
    
    // Install the recurring trigger that will drive the batch processing.
    AHA_InstallTrigger2();
    
    // Sort the list of files to be imported for better organization.
    AHA_SortValidationList2();

  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}















