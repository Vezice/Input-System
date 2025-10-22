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



function AHA_StartImport2() {
  const start = new Date();
  try {
    // --- MODIFICATION ---
    PropertiesService.getScriptProperties().setProperty('SYSTEM_STATUS', 'IMPORTING');
    AHA_SlackNotify3("⚠️ *Starting Import...* Status: IMPORTING");
    
    AHA_InstallTrigger2();
    AHA_SortValidationList2();

  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}


/**
 * A helper function to check if a trigger for a specific function exists.
 * @param {string} functionName The name of the function to check for.
 * @returns {boolean} True if a trigger exists, false otherwise.
 */
function AHA_CheckForTrigger(functionName) {
  try {
    const triggers = ScriptApp.getProjectTriggers();
    return triggers.some(trigger => trigger.getHandlerFunction() === functionName);
  } catch (e) {
    Logger.log(`Error checking for trigger ${functionName}: ${e.message}`);
    return false; // Fail safe
  }
}

function AHA_SystemWatchdog() {
  // --- MODIFICATION ---
  const status = PropertiesService.getScriptProperties().getProperty('SYSTEM_STATUS');

  if (!status) { // If status is null or deleted, worker is OFFLINE
    Logger.log("Watchdog: System is OFFLINE. No action needed.");
    // In this new design, the watchdog should also delete itself if it finds it's
    // running while the system is offline (e.g., after a manual stop).
    AHA_DeleteTriggers2("AHA_SystemWatchdog");
    return;
  }
  
  if (status === "VALIDATING") {
    if (!AHA_CheckForTrigger("AHA_RunValBatchSafely2")) {
      AHA_SlackNotify3("⚠️ *Watchdog Alert*: System was stuck in VALIDATING. Restarting validation trigger. <@U08TUF8LW2H>");
      AHA_StartValTrigger2(1);
    }
  }

  if (status === "IMPORTING") {
    if (!AHA_CheckForTrigger("AHA_RunImportBatchSafely2")) {
      AHA_SlackNotify3("⚠️ *Watchdog Alert*: System was stuck in IMPORTING. Restarting import trigger. <@U08TUF8LW2H>");
      AHA_InstallTrigger2();
    }
  }

  const cleanupStatuses = ["FINALIZING", "ARCHIVING", "CLEANUP"];
  if (cleanupStatuses.includes(status)) {
    let handlerFunction = "";
    if (status === "FINALIZING") handlerFunction = "AHA_RunFinalization";
    if (status === "ARCHIVING") handlerFunction = "AHA_RunArchiving";
    // Note: 'CLEANUP' is so fast, we just let it restart at 'ARCHIVING'
    if (status === "CLEANUP") handlerFunction = "AHA_RunArchiving";

    if (handlerFunction && !AHA_CheckForTrigger(handlerFunction)) {
       AHA_SlackNotify3(`⚠️ *Watchdog Alert*: System was stuck in ${status}. Restarting failed step. <@U08TUF8LW2H>`);
       ScriptApp.newTrigger(handlerFunction)
         .timeBased()
         .after(30 * 1000) 
         .create();
    }
  }
}


/**
 * --- SAFE BATCH RUNNER (MODIFIED) ---
 * This function now calls the staged cleanup process instead of
 * the old monolithic function.
 */
function AHA_RunImportBatchSafely2() {
    const start = new Date();
    try {
        const ss = SpreadsheetApp.getActiveSpreadsheet();
        const logSheet = ss.getSheetByName("Logs");
        const lock = LockService.getScriptLock();
        
        const maxRetries = 3; 
        const retryDelay = 2 * 60 * 1000; 

        let lockAcquired = false;
        for (let attempt = 0; attempt < maxRetries; attempt++) {
            if (lock.tryLock(10000)) {
                lockAcquired = true;
                break; 
            }
            if (attempt < maxRetries - 1) {
                Logger.log(`Lock is busy. Retrying in 2 minutes... (Attempt ${attempt + 1}/${maxRetries})`);
                Utilities.sleep(retryDelay);
            }
        }

        if (!lockAcquired) {
            Logger.log(`Could not acquire lock after ${maxRetries} attempts. Aborting.`);
            AHA_SlackNotify3("⚠️ *Trigger cancelled*: Could not acquire lock after multiple attempts.");
            return;
        }

        try {
            if (AHA_CekStatusImport2()) {
                AHA_ImportByCategoryBatch2();
            } else {
                Logger.log("No files left to process. Starting final cleanup process...");
                logSheet.appendRow([new Date(), "Status", "✅ All Files processed. Starting Cleanup."]);
                AHA_SlackNotify3("✅ *Completed* : All Files has been processed! Starting Cleanup...");
                
                // --- MODIFICATION ---
                // Instead of calling the old function, start the new staged process.
                AHA_StartCleanupProcess();
            }
        } catch (err) {
            Logger.log("Error in processNextBatch: " + err);
        } finally {
            lock.releaseLock();
        }

    } finally {
        const end = new Date();
        AHA_LogRuntime3(end - start);
    }
}

function AHA_StartCleanupProcess() {
  const triggers = ScriptApp.getProjectTriggers();
  for (let t of triggers) {
    if (t.getHandlerFunction() === "AHA_RunImportBatchSafely2") {
      ScriptApp.deleteTrigger(t);
      Logger.log("Deleted recurring import trigger.");
      break;
    }
  }

  // --- MODIFICATION ---
  PropertiesService.getScriptProperties().setProperty('SYSTEM_STATUS', 'FINALIZING');
  
  ScriptApp.newTrigger("AHA_RunFinalization")
    .timeBased()
    .after(10 * 1000)
    .create();
}

/**
 * --- NEW FUNCTION: STEP 2 (FINALIZE SHEETS) ---
 * Replaces the first part of the old AHA_RemoveAllTriggers2
 */
function AHA_RunFinalization() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const inputSheet = ss.getSheetByName("Input");
  
  try {
    if (CONFIG.CATEGORY_IMPORTING_ENABLED) {
      AHA_FinalizeAllTempSheets2();
    } else {
      // Fallback for old system (if needed)
      const categoriesRange = inputSheet.getRange("E5:E" + inputSheet.getLastRow()).getValues();
      const allCategories = [...new Set(categoriesRange.flat().filter(Boolean))];
      allCategories.forEach(category => {
        try { AHA_FinalizeSheetSwap2(category); } 
        catch (e) { Logger.log(`❌ Failed to finalize sheet swap for category ${category}: ${e.message}`); }
      });
    }

    // Success: Set status and trigger next step
    PropertiesService.getScriptProperties().setProperty('SYSTEM_STATUS', 'ARCHIVING');
    ScriptApp.newTrigger("AHA_RunArchiving")
      .timeBased()
      .after(10 * 1000)
      .create();

  } catch (e) {
    AHA_SlackNotify3(`❌ *Error*: Cleanup Step 'Finalization' failed: ${e.message} <@U08TUF8LW2H>`);
    // Do not proceed. The watchdog will restart this step.
  }
}

/**
 * --- NEW FUNCTION: STEP 3 & 4 (ARCHIVE & CLEANUP) ---
 * Replaces the rest of the old AHA_RemoveAllTriggers2
 */
function AHA_RunArchiving() {
  try {
    // --- MODIFICATION ---
    PropertiesService.getScriptProperties().setProperty('SYSTEM_STATUS', 'CLEANUP');
    
    AHA_SlackNotify3("⚠️ *Archiving Files...*");
    AHA_ArchiveFilesByCategory2();

    AHA_DeleteUnusedTempSheets2(); 
    AHA_SlackNotify3("✅ *Import Completed* ✅");

    ScriptApp.newTrigger('AHA_NotifyCentral3')
      .timeBased()
      .after(60 * 1000)
      .create();

    // --- FINAL RESET ---
    // 1. Delete the status property to signal "OFFLINE"
    PropertiesService.getScriptProperties().deleteProperty('SYSTEM_STATUS');
    Logger.log("SYSTEM_STATUS property deleted. Worker is now OFFLINE.");

    // 2. Delete the Watchdog trigger
    AHA_DeleteTriggers2("AHA_SystemWatchdog");
    Logger.log("Watchdog trigger deleted. Worker process complete.");

  } catch (e) {
    AHA_SlackNotify3(`❌ *Error*: Cleanup Step 'Archiving' failed: ${e.message} <@U08TUF8LW2H>`);
  }
}









