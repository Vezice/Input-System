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


/**
 * Checks if all temporary sheets are ready by reading the TempControl sheet.
 * @returns {boolean} True if all temp sheets have "Done" status, false otherwise.
 */
function AHA_AreTempSheetsReady2() {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const controlSheet = ss.getSheetByName("TempControl");

    if (!controlSheet) {
      Logger.log("TempControl sheet not found. Assuming not ready.");
      return false;
    }

    const lastRow = controlSheet.getLastRow();
    if (lastRow < 2) {
      Logger.log("TempControl sheet is empty. Assuming not ready.");
      return false;
    }

    const data = controlSheet.getRange(2, 1, lastRow - 1, 2).getValues();

    for (const row of data) {
      const sheetName = row[0];
      const status = row[1];
      if (sheetName && status !== "Done") {
        Logger.log(`Temp sheet "${sheetName}" is not ready. Status: ${status}`);
        return false;
      }
    }

    return true; // All sheets are "Done"
  } catch (e) {
    Logger.log(`Error checking temp sheet status: ${e.message}`);
    return false;
  }
}

function AHA_StartImport2() {
  const start = new Date();
  try {
    // --- NEW: Wait for temp sheets to be ready before importing ---
    if (!AHA_AreTempSheetsReady2()) {
      AHA_SlackNotify3("⏳ Waiting for temp sheets to be created before importing...");

      // Delete any existing retry triggers to prevent duplicates
      AHA_DeleteTriggers2("AHA_StartImport2");

      // Create a trigger to retry after 30 seconds
      ScriptApp.newTrigger("AHA_StartImport2")
        .timeBased()
        .after(30 * 1000) // 30 seconds
        .create();

      return; // Exit and wait for retry
    }
    // --- END NEW ---

    // Clean up any leftover retry triggers
    AHA_DeleteTriggers2("AHA_StartImport2");

    PropertiesService.getScriptProperties().deleteProperty("RESTART_COUNT_VALIDATING");
    PropertiesService.getScriptProperties().deleteProperty("QUARANTINE_COUNT");
    PropertiesService.getScriptProperties().setProperty('SYSTEM_STATUS', 'IMPORTING');

    // --- NEW: Set the initial import heartbeat ---
    PropertiesService.getScriptProperties().setProperty("LAST_IMPORT_HEARTBEAT", new Date().getTime());

    AHA_SlackNotify3("✅ *Temp sheets ready!* Starting Import... Status: IMPORTING");

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

/**
 * --- WATCHDOG FUNCTION (UPGRADED WITH "STALENESS" CHECK) ---
 * Runs on a schedule to check if the system is stuck (stale).
 * A "stale" process is one that has not updated its "heartbeat" timestamp.
 */
function AHA_SystemWatchdog() {
  const properties = PropertiesService.getScriptProperties();
  const status = properties.getProperty('SYSTEM_STATUS');
  const STALENESS_LIMIT_MS = 10 * 60 * 1000; // 10 minutes
  const MAX_RESTARTS = 3;

  // --- TRIGGER OVERFLOW CHECK ---
  // Apps Script has a limit of 20 triggers per user per script.
  // If we're approaching this limit, clean up and recreate only essential triggers.
  const TRIGGER_WARNING_THRESHOLD = 15;
  const allTriggers = ScriptApp.getProjectTriggers();
  const triggerCount = allTriggers.length;

  if (triggerCount >= TRIGGER_WARNING_THRESHOLD) {
    AHA_SlackNotify3(`⚠️ *Watchdog Alert*: Trigger overflow detected (${triggerCount} triggers). Cleaning up... <@U0A6B24777X>`);
    AHA_CleanupAndRecreateEssentialTriggers(status);
    return; // Exit after cleanup - the recreated watchdog will continue monitoring
  } 

  if (!status) {
    // System is OFFLINE - but check if there are pending files that need processing
    try {
      const moveFolderId = properties.getProperty("MOVE_FOLDER_ID");
      if (moveFolderId) {
        const folder = DriveApp.getFolderById(moveFolderId);
        const files = folder.getFiles();
        if (files.hasNext()) {
          // There ARE pending files but system is OFFLINE - this is wrong!
          const workerName = properties.getProperty("WORKER_NAME") || "Unknown Worker";
          AHA_SlackNotify3(`🚨 *Watchdog Alert*: ${workerName} is OFFLINE but has pending files in Move folder! Restarting validation... <@U0A6B24777X>`);
          properties.deleteProperty("RESTART_COUNT_VALIDATING");
          properties.deleteProperty("RESTART_COUNT_IMPORTING");
          AHA_StartValTrigger2(1); // Restart the process
          return;
        }
      }
    } catch (e) {
      // Folder ID might be stale/invalid - clear it
      properties.deleteProperty("MOVE_FOLDER_ID");
    }

    // No pending files - truly OFFLINE, safe to delete watchdog
    Logger.log("Watchdog: System is OFFLINE with no pending files. Deleting self.");
    AHA_DeleteTriggers2("AHA_SystemWatchdog");
    return;
  }
  
  // --- VALIDATING STAGE CHECK ---
  if (status === "VALIDATING") {
    const lastHeartbeat = Number(properties.getProperty("LAST_VALIDATION_HEARTBEAT") || 0);
    const now = new Date().getTime();

    // Check if the last heartbeat is older than the staleness limit
    if (lastHeartbeat === 0 || (now - lastHeartbeat) > STALENESS_LIMIT_MS) {
      // The system is STALE (crashed or stuck). Time to act.
      const restartCount = Number(properties.getProperty("RESTART_COUNT_VALIDATING") || 0);

      if (restartCount >= MAX_RESTARTS) {
        // --- QUARANTINE ---
        Logger.log(`CRITICAL: VALIDATING stage has been stale for 10+ minutes and failed ${restartCount} restarts. Triggering quarantine.`);
        AHA_SlackNotify3(`🚨 *CRITICAL FAILURE*: System is STALE. Restart attempts failed. Triggering "Poison Pill" Quarantine. <@U0A6B24777X>`);
        AHA_QuarantinePoisonPill(); // This is the function from our previous discussion
        // Clear stale folder ID to prevent "Invalid argument: id" errors
        properties.deleteProperty("MOVE_FOLDER_ID");
        AHA_StartValTrigger2(1);    // Restart *after* quarantining
      } else {
        // --- RESTART ---
        const newCount = restartCount + 1;
        properties.setProperty("RESTART_COUNT_VALIDATING", newCount);
        // Force-delete any lingering triggers, just in case
        AHA_DeleteTriggers2("AHA_RunValBatchSafely2");
        // Clear stale folder ID to prevent "Invalid argument: id" errors on restart
        properties.deleteProperty("MOVE_FOLDER_ID");
        AHA_SlackNotify3(`⚠️ *Watchdog Alert*: System is STALE (no heartbeat in 10 min). Restarting... (Attempt ${newCount}/${MAX_RESTARTS})`);
        AHA_StartValTrigger2(1); // Restart the process
      }
    } else {
      // System is NOT stale. It's actively working. Do nothing.
      Logger.log("Watchdog: System is VALIDATING and heartbeat is current. All good.");
    }
  }

  // --- IMPORTING STAGE CHECK ---
  if (status === "IMPORTING") {
    const lastHeartbeat = Number(properties.getProperty("LAST_IMPORT_HEARTBEAT") || 0);
    const now = new Date().getTime();

    if (lastHeartbeat === 0 || (now - lastHeartbeat) > STALENESS_LIMIT_MS) {
      const restartCount = Number(properties.getProperty("RESTART_COUNT_IMPORTING") || 0);
      if (restartCount >= MAX_RESTARTS) {
        AHA_SlackNotify3(`🚨 *CRITICAL FAILURE*: IMPORTING process is STALE and failed ${restartCount} restarts. Shutting down. <@U0A6B24777X>`);
        AHA_RunArchiving(); // Shut down
      } else {
        const newCount = restartCount + 1;
        properties.setProperty("RESTART_COUNT_IMPORTING", newCount);
        AHA_DeleteTriggers2("AHA_RunImportBatchSafely2");
        AHA_SlackNotify3(`⚠️ *Watchdog Alert*: IMPORTING is STALE (no heartbeat in 10 min). Restarting... (Attempt ${newCount}/${MAX_RESTARTS})`);
        AHA_InstallTrigger2(); // Restart the process
      }
    } else {
       Logger.log("Watchdog: System is IMPORTING and heartbeat is current. All good.");
    }
  }
  
  // --- CLEANUP STAGE CHECK (no heartbeat needed, just check for triggers) ---
  const cleanupStatuses = ["FINALIZING", "ARCHIVING", "CLEANUP"];
  if (cleanupStatuses.includes(status)) {
    let handlerFunction = "";
    if (status === "FINALIZING") handlerFunction = "AHA_RunFinalization";
    if (status === "ARCHIVING") handlerFunction = "AHA_RunArchiving";
    if (status === "CLEANUP") handlerFunction = "AHA_RunArchiving"; 

    if (handlerFunction && !AHA_CheckForTrigger(handlerFunction)) {
       AHA_SlackNotify3(`⚠️ *Watchdog Alert*: System was stuck in ${status}. Restarting failed step.`);
       ScriptApp.newTrigger(handlerFunction)
         .timeBased()
         .after(10 * 1000) 
         .create();
    }
  }
}

/**
 * --- NEW: POISON PILL HANDLER ---
 * This function is called by the Watchdog when it detects a persistent crash loop.
 * It finds the "Move" folder, quarantines the *first file* in the list, logs it,
 * and then allows the Watchdog to restart the process.
 */
function AHA_QuarantinePoisonPill() {
  try {
    const properties = PropertiesService.getScriptProperties();
    const moveFolderId = properties.getProperty("MOVE_FOLDER_ID");
    if (!moveFolderId) {
      Logger.log("Quarantine skipped: No MOVE_FOLDER_ID found in properties.");
      return;
    }
    
    const folder = DriveApp.getFolderById(moveFolderId);
    const files = folder.getFiles();

    if (files.hasNext()) {
      const poisonFile = files.next();
      const fileName = poisonFile.getName();

      // Track total quarantines to prevent infinite loops
      const quarantineCount = Number(properties.getProperty("QUARANTINE_COUNT") || 0) + 1;
      properties.setProperty("QUARANTINE_COUNT", String(quarantineCount));

      if (quarantineCount >= 5) {
        // Too many quarantines in one session - stop the system entirely
        AHA_SlackNotify3(`🚨 *SYSTEM HALTED*: ${quarantineCount} files quarantined in one session. Manual intervention required. <@U0A6B24777X>`);
        properties.deleteProperty("RESTART_COUNT_VALIDATING");
        properties.deleteProperty('SYSTEM_STATUS');
        AHA_DeleteTriggers2("AHA_RunValBatchSafely2");
        // Still move this file to Failed before halting
        const parentFolder = DriveApp.getFolderById(CONFIG.FOLDER_IDS.ROOT_SHARED_DRIVE);
        const failedFolder = AHA_GetSubFolder2(parentFolder, "Failed");
        poisonFile.moveTo(failedFolder);
        AHA_LogFailureToDoc(fileName, `Poison Pill Quarantine #${quarantineCount} (System Halted)`, properties.getProperty("WORKER_CATEGORY") || "Unknown", properties.getProperty("WORKER_COUNT") || "System Watchdog");
        return; // Don't restart - wait for human intervention
      }

      // 1. Move the file to the Failed folder
      const parentFolder = DriveApp.getFolderById(CONFIG.FOLDER_IDS.ROOT_SHARED_DRIVE);
      const failedFolder = AHA_GetSubFolder2(parentFolder, "Failed");
      poisonFile.moveTo(failedFolder);

      // 2. Log this action to the Google Doc
      const workerName = properties.getProperty("WORKER_COUNT") || "System Watchdog";
      const category = properties.getProperty("WORKER_CATEGORY") || "Unknown";
      AHA_LogFailureToDoc(fileName, "Poison Pill Quarantine (Auto-Removed by Watchdog)", category, workerName);

      // 3. Send a critical alert
      AHA_SlackNotify3(`🚨 *POISON PILL QUARANTINED*: The file *${fileName}* was causing a persistent crash loop and has been moved to the Failed folder. The system will now retry. <@U0A6B24777X>`);
      Logger.log(`🚨 QUARANTINED: ${fileName}`);

      // 4. Reset the restart counter so the system can try again
      properties.deleteProperty("RESTART_COUNT_VALIDATING");

    } else {
      Logger.log("Watchdog triggered quarantine, but no files were found in the Move folder.");
    }
  } catch (err) {
    Logger.log(`CRITICAL ERROR in AHA_QuarantinePoisonPill: ${err.message}`);
    AHA_SlackNotify3(`❌ *CRITICAL FAILURE* in Watchdog's quarantine function: ${err.message} <@U0A6B24777X>`);
  }
}

/**
 * --- SAFE BATCH RUNNER (MODIFIED) ---
 * -- MODIFIED to update the heartbeat on lock failure to prevent false "stale" state. --
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

            // --- THE FIX ---
            // Update the heartbeat. This tells the Watchdog "I'm alive, just locked."
            PropertiesService.getScriptProperties().setProperty("LAST_IMPORT_HEARTBEAT", new Date().getTime());
            // --- END FIX ---

            return;
        }

        try {
            if (AHA_CekStatusImport2()) {
                AHA_ImportByCategoryBatch2();
            } else {
                Logger.log("No files left to process. Starting final cleanup process...");
                logSheet.appendRow([new Date(), "Status", "✅ All Files processed. Starting Cleanup."]);
                AHA_SlackNotify3("✅ *Completed* : All Files has been processed! Starting Cleanup...");
                
                AHA_StartCleanupProcess();
            }
        } catch (err) {
            Logger.log("Error in processNextBatch: " + err);
            // --- THE FIX (PART 2) ---
            PropertiesService.getScriptProperties().setProperty("LAST_IMPORT_HEARTBEAT", new Date().getTime());
            // --- END FIX ---
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
  const MAX_RETRIES = 3;
  const RETRY_DELAY_MS = 10000; // 10 seconds between retries
  const properties = PropertiesService.getScriptProperties();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const inputSheet = ss.getSheetByName("Input");

  // Track retry count across executions
  let retryCount = Number(properties.getProperty("FINALIZATION_RETRY_COUNT") || 0);

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
    properties.setProperty('SYSTEM_STATUS', 'ARCHIVING');
    properties.deleteProperty('FINALIZATION_RETRY_COUNT'); // Clear retry counter on success
    ScriptApp.newTrigger("AHA_RunArchiving")
      .timeBased()
      .after(10 * 1000)
      .create();

  } catch (e) {
    const errorMsg = e.message || String(e);

    // Check if this is a transient error that we should retry
    const isTransientError = errorMsg.includes("Service error") ||
                             errorMsg.includes("Drive") ||
                             errorMsg.includes("Sheets") ||
                             errorMsg.includes("temporarily unavailable") ||
                             errorMsg.includes("server error") ||
                             errorMsg.includes("timed out");

    if (isTransientError && retryCount < MAX_RETRIES) {
      retryCount++;
      properties.setProperty("FINALIZATION_RETRY_COUNT", String(retryCount));

      Logger.log(`⚠️ Finalization failed with transient error. Scheduling retry ${retryCount}/${MAX_RETRIES}...`);
      AHA_SlackNotify3(`⚠️ *Finalization* hit a transient error: "${errorMsg}". Retrying (${retryCount}/${MAX_RETRIES})...`);

      // Schedule a retry after delay
      ScriptApp.newTrigger('AHA_RunFinalization')
        .timeBased()
        .after(RETRY_DELAY_MS)
        .create();
    } else {
      // Max retries exceeded or non-transient error
      properties.deleteProperty('FINALIZATION_RETRY_COUNT');
      AHA_SlackNotify3(`❌ *Error*: Cleanup Step 'Finalization' failed: ${errorMsg} <@U0A6B24777X>`);
      // Do not proceed. The watchdog will restart this step if needed.
    }
  }
}

/**
 * --- NEW FUNCTION: STEP 3 & 4 (ARCHIVE & CLEANUP) ---
 * Replaces the rest of the old AHA_RemoveAllTriggers2
 * * -- MODIFIED to include a final "goodbye" message --
 */
function AHA_RunArchiving() {
  const MAX_RETRIES = 3;
  const RETRY_DELAY_MS = 10000; // 10 seconds between retries
  const properties = PropertiesService.getScriptProperties();

  // Track retry count across executions (in case of timeout)
  let retryCount = Number(properties.getProperty("ARCHIVING_RETRY_COUNT") || 0);

  try {
    // --- MODIFICATION ---
    properties.setProperty('SYSTEM_STATUS', 'CLEANUP');

    AHA_SlackNotify3("⚠️ *Archiving Files...*");
    AHA_ArchiveFilesByCategory2();

    // --- NEW: Sweep for orphaned files that weren't properly tracked ---
    // This catches files that were validated but the script crashed before
    // recording them in the Input sheet, so they were never imported/archived.
    AHA_SweepOrphanedFiles();

    AHA_DeleteUnusedTempSheets2();
    AHA_SlackNotify3("✅ *Import Completed* ✅");

    ScriptApp.newTrigger('AHA_NotifyCentral3')
      .timeBased()
      .after(60 * 1000)
      .create();

    // --- [THIS IS THE NEW LINE YOU ADDED] ---
    // Send the friendly "hi" / "goodbye" message to the user
    // AHA_SayGoodbye();
    // --- [END OF NEW LINE] ---

    // --- FINAL RESET ---
    // 1. Delete the status property to signal "OFFLINE"
    properties.deleteProperty('SYSTEM_STATUS');
    properties.deleteProperty('ARCHIVING_RETRY_COUNT'); // Clear retry counter on success
    Logger.log("SYSTEM_STATUS property deleted. Worker is now OFFLINE.");

    // 2. Delete the Watchdog trigger
    AHA_DeleteTriggers2("AHA_SystemWatchdog");
    Logger.log("Watchdog trigger deleted. Worker process complete.");

  } catch (e) {
    const errorMsg = e.message || String(e);

    // Check if this is a transient Drive/Service error that we should retry
    const isTransientError = errorMsg.includes("Service error") ||
                             errorMsg.includes("Drive") ||
                             errorMsg.includes("temporarily unavailable") ||
                             errorMsg.includes("server error") ||
                             errorMsg.includes("timed out");

    if (isTransientError && retryCount < MAX_RETRIES) {
      retryCount++;
      properties.setProperty("ARCHIVING_RETRY_COUNT", String(retryCount));

      Logger.log(`⚠️ Archiving failed with transient error. Scheduling retry ${retryCount}/${MAX_RETRIES}...`);
      AHA_SlackNotify3(`⚠️ *Archiving* hit a transient error: "${errorMsg}". Retrying (${retryCount}/${MAX_RETRIES})...`);

      // Schedule a retry after delay
      ScriptApp.newTrigger('AHA_RunArchiving')
        .timeBased()
        .after(RETRY_DELAY_MS)
        .create();
    } else {
      // Max retries exceeded or non-transient error
      properties.deleteProperty('ARCHIVING_RETRY_COUNT');
      AHA_SlackNotify3(`❌ *Error*: Cleanup Step 'Archiving' failed: ${errorMsg} <@U0A6B24777X>`);
    }
  }
}

/**
 * --- MASTER RESET FOR TRIGGERS ---
 * Deletes ALL programmatic time-based triggers for this script.
 * This is a "clean slate" function to prevent "too many triggers" errors
 * from "zombie" triggers left over after a crash.
 */
function AHA_DeleteAllTimeBasedTriggers() {
  let deletedCount = 0;
  try {
    const allTriggers = ScriptApp.getProjectTriggers();
    for (const trigger of allTriggers) {
      // We only delete time-based (CLOCK) triggers.
      // This protects your web app (doPost) and any manual triggers.
      if (trigger.getTriggerSource() === ScriptApp.TriggerSource.CLOCK) {
        ScriptApp.deleteTrigger(trigger);
        deletedCount++;
      }
    }
    if (deletedCount > 0) {
      Logger.log(`AHA_DeleteAllTimeBasedTriggers: Deleted ${deletedCount} old (CLOCK) triggers.`);
    }
  } catch (e) {
    Logger.log(`Error in AHA_DeleteAllTimeBasedTriggers: ${e.message}`);
    // We send a low-priority alert but don't stop the script.
    AHA_SlackNotify3(`⚠️ A minor error occurred while cleaning old triggers: ${e.message}`);
  }
}


/**
 * --- TRIGGER OVERFLOW CLEANUP ---
 * Called by the Watchdog when trigger count exceeds threshold.
 * Deletes ALL time-based triggers and recreates only the essential ones
 * based on the current SYSTEM_STATUS.
 *
 * @param {string} currentStatus The current SYSTEM_STATUS value.
 */
function AHA_CleanupAndRecreateEssentialTriggers(currentStatus) {
  try {
    // Step 1: Delete ALL time-based triggers (clean slate)
    AHA_DeleteAllTimeBasedTriggers();
    Logger.log("Trigger cleanup: All time-based triggers deleted.");

    // Step 2: Recreate only the essential triggers based on current status
    // Always recreate the watchdog first
    ScriptApp.newTrigger("AHA_SystemWatchdog")
      .timeBased()
      .everyMinutes(15)
      .create();
    Logger.log("Trigger cleanup: Watchdog trigger recreated.");

    // Step 3: Recreate the phase-specific trigger
    if (!currentStatus) {
      // System is OFFLINE - no phase trigger needed
      Logger.log("Trigger cleanup: System is OFFLINE, no phase trigger needed.");
      AHA_SlackNotify3("✅ *Trigger Cleanup Complete*: System was OFFLINE. Only watchdog recreated.");
      return;
    }

    switch (currentStatus) {
      case "VALIDATING":
        // Recreate validation batch trigger
        ScriptApp.newTrigger("AHA_RunValBatchSafely2")
          .timeBased()
          .everyMinutes(5)
          .create();
        Logger.log("Trigger cleanup: Validation trigger recreated.");
        AHA_SlackNotify3("✅ *Trigger Cleanup Complete*: Recreated watchdog + validation trigger. Resuming VALIDATING phase.");
        break;

      case "IMPORTING":
        // Recreate import batch trigger
        ScriptApp.newTrigger("AHA_RunImportBatchSafely2")
          .timeBased()
          .everyMinutes(5)
          .create();
        Logger.log("Trigger cleanup: Import trigger recreated.");
        AHA_SlackNotify3("✅ *Trigger Cleanup Complete*: Recreated watchdog + import trigger. Resuming IMPORTING phase.");
        break;

      case "FINALIZING":
        // Recreate finalization trigger (one-time)
        ScriptApp.newTrigger("AHA_RunFinalization")
          .timeBased()
          .after(10 * 1000)
          .create();
        Logger.log("Trigger cleanup: Finalization trigger recreated.");
        AHA_SlackNotify3("✅ *Trigger Cleanup Complete*: Recreated watchdog + finalization trigger. Resuming FINALIZING phase.");
        break;

      case "ARCHIVING":
      case "CLEANUP":
        // Recreate archiving trigger (one-time)
        ScriptApp.newTrigger("AHA_RunArchiving")
          .timeBased()
          .after(10 * 1000)
          .create();
        Logger.log("Trigger cleanup: Archiving trigger recreated.");
        AHA_SlackNotify3(`✅ *Trigger Cleanup Complete*: Recreated watchdog + archiving trigger. Resuming ${currentStatus} phase.`);
        break;

      default:
        // Unknown status - just keep watchdog running
        Logger.log(`Trigger cleanup: Unknown status '${currentStatus}'. Only watchdog recreated.`);
        AHA_SlackNotify3(`⚠️ *Trigger Cleanup*: Unknown status '${currentStatus}'. Only watchdog recreated. Manual intervention may be needed.`);
    }

  } catch (e) {
    Logger.log(`CRITICAL ERROR in AHA_CleanupAndRecreateEssentialTriggers: ${e.message}`);
    AHA_SlackNotify3(`❌ *CRITICAL*: Trigger cleanup failed: ${e.message} <@U0A6B24777X>`);
  }
}







