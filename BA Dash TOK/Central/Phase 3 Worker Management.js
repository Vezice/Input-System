////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Worker Management.gs
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// iBot v2 GCS Upload Configuration
const IBOT_V2_GCS_CONFIG = {
  ENABLED: true,
  UPLOAD_ENDPOINT: 'https://asia-southeast2-fbi-dev-484410.cloudfunctions.net/ibot-v2-http/upload',
};

/**
 * Uploads all files from the Upload Here folder to GCS for iBot v2 processing.
 * This runs in parallel with v1 processing - failures here don't block v1.
 *
 * @param {string} category The category name (e.g., "BA Produk SHO")
 * @param {GoogleAppsScript.Drive.Folder} uploadHereFolder The folder containing files to upload
 * @returns {object} Result with counts of successful and failed uploads
 */
function AHA_UploadFilesToGCS3(category, uploadHereFolder) {
  if (!IBOT_V2_GCS_CONFIG.ENABLED) {
    Logger.log("GCS upload disabled, skipping...");
    return { uploaded: 0, failed: 0, skipped: true };
  }

  const files = uploadHereFolder.getFiles();
  let uploaded = 0;
  let failed = 0;
  const errors = [];

  while (files.hasNext()) {
    const file = files.next();
    const filename = file.getName();

    // Skip non-data files
    if (!filename.match(/\.(xlsx|xls|csv)$/i)) {
      continue;
    }

    try {
      const blob = file.getBlob();
      const content = Utilities.base64Encode(blob.getBytes());

      const payload = {
        category: category,
        filename: filename,
        content: content
      };

      const options = {
        method: 'post',
        contentType: 'application/json',
        payload: JSON.stringify(payload),
        muteHttpExceptions: true
      };

      const response = UrlFetchApp.fetch(IBOT_V2_GCS_CONFIG.UPLOAD_ENDPOINT, options);
      const responseCode = response.getResponseCode();

      if (responseCode === 200) {
        uploaded++;
        Logger.log(`✅ GCS upload: ${filename}`);
      } else {
        failed++;
        errors.push(`${filename}: ${response.getContentText()}`);
        Logger.log(`❌ GCS upload failed: ${filename} - ${response.getContentText()}`);
      }
    } catch (e) {
      failed++;
      errors.push(`${filename}: ${e.message}`);
      Logger.log(`❌ GCS upload error: ${filename} - ${e.message}`);
    }
  }

  Logger.log(`GCS upload complete: ${uploaded} uploaded, ${failed} failed`);

  // Don't send Slack notification for GCS failures - just log them
  // v1 should continue processing regardless of GCS upload status

  return { uploaded, failed, errors };
}

/**
 * Checks the 'Upload Here' folder for duplicate filenames.
 * If duplicates are found, sends a Slack alert with details and returns false.
 * This prevents the system from processing duplicate files that would cause
 * duplicate entries in the final sheets.
 *
 * @param {GoogleAppsScript.Drive.Folder} uploadHereFolder The folder to check.
 * @param {string} category The category name for the Slack message.
 * @returns {boolean} Returns true if no duplicates found, false if duplicates exist.
 */
function AHA_CheckForDuplicateFiles3(uploadHereFolder, category) {
  const fileIterator = uploadHereFolder.getFiles();
  const filenameCounts = new Map();

  // Count occurrences of each filename
  while (fileIterator.hasNext()) {
    const file = fileIterator.next();
    const filename = file.getName();
    filenameCounts.set(filename, (filenameCounts.get(filename) || 0) + 1);
  }

  // Find duplicates (count > 1)
  const duplicates = [];
  for (const [filename, count] of filenameCounts.entries()) {
    if (count > 1) {
      duplicates.push({ filename, count });
    }
  }

  if (duplicates.length > 0) {
    // Build alert message
    let message = `🚨 *DUPLICATE FILES DETECTED* for *${category}*!\n\n`;
    message += `The following files have duplicates in the 'Upload Here' folder:\n`;

    duplicates.forEach(dup => {
      message += `• *${dup.filename}* (${dup.count} copies)\n`;
    });

    message += `\n⛔ *Import process halted.* Please remove the duplicate files and try again.\n`;
    message += `\n💡 *Tip:* Use \`/ibot removeduplicates ${category}\` to automatically remove duplicates.\n`;
    message += `<@U0A6B24777X>`;

    AHA_SlackNotify3(message);
    Logger.log(`Duplicate files detected: ${JSON.stringify(duplicates)}`);

    return false; // Duplicates found
  }

  Logger.log("✅ No duplicate filenames detected. Proceeding with import.");
  return true; // No duplicates
}

/**
 * Main function to start the worker process. It now checks if there are files
 * to process before triggering the workers.
 */
function AHA_StartWorkers3() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  AHA_SetConfigurationForCategory3();
  try {
    const dashboardSheet = ss.getSheetByName("System Dashboard");
    if (!dashboardSheet) {
      throw new Error("Sheet 'System Dashboard' not found.");
    }
    const targetCategory = dashboardSheet.getRange("E1").getValue();
    if (!targetCategory) {
      throw new Error("Category is not set in 'System Dashboard' cell E1.");
    }
    Logger.log(`Running process for category: ${targetCategory}`);
    PropertiesService.getScriptProperties().setProperty("CENTRAL_CATEGORY", targetCategory);

    AHA_ResetWorkerCopyStatus(ss, targetCategory);
    
    // --- KEY CHANGE 1: Capture the return value ---
    // The split function now returns 'true' if files were moved, and 'false' otherwise.
    const filesWereSplit = AHA_SplitFilesToWorkers3(ss, targetCategory);

    // --- KEY CHANGE 2: Conditionally trigger workers ---
    if (filesWereSplit) {
      Logger.log("Files were successfully split. Proceeding to trigger workers.");
      AHA_TriggerAllWorkers3(ss, targetCategory);
    } else {
      // If no files were split, log it and end the process gracefully.
      // The Slack notification is already handled inside the split function.
      Logger.log("Halting process because no files were found to split.");
    }

  } catch (e) {
    const errorMessage = `❌ Error in AHA_StartWorkers3: ${e.message}`;
    Logger.log(errorMessage);
    AHA_SlackNotify3(`${errorMessage} <@U0A6B24777X>`); // Notify with specific error
  }
}


/**
 * Splits files from the 'Upload Here' folder into worker folders.
 * -- MODIFIED with a 5-attempt retry loop to handle leftover files. --
 *
 * @param {GoogleAppsScript.Spreadsheet.Spreadsheet} ss The active spreadsheet object.
 * @param {string} category The target category to process.
 * @returns {boolean} Returns true if files were found and split, otherwise false.
 */
function AHA_SplitFilesToWorkers3(ss, category) {
  const updateSheet = ss.getSheetByName("System Dashboard");
  updateSheet.getRange("A1").setValue("SCRIPT ONLINE");

  const moveFolderId = '1zQEPDi4dI3gUJiEKYp-GQI8lfNFwL1sh'; // Base folder for all categories
  const moveFolder = DriveApp.getFolderById(moveFolderId);
  if (!moveFolder) {
    const errorMsg = `❌ Error: Base 'Move' folder not found by ID: ${moveFolderId}.`;
    AHA_SlackNotify3(`${errorMsg} <@U0A6B24777X>`);
    throw new Error(errorMsg);
  }

  const categoryFolders = moveFolder.getFoldersByName(category);
  if (!categoryFolders.hasNext()) {
    throw new Error(`Category folder "${category}" not found.`);
  }
  const baseFolder = categoryFolders.next();

  const uploadHereFolders = baseFolder.getFoldersByName('Upload Here');
  if (!uploadHereFolders.hasNext()) {
    throw new Error(`"Upload Here" folder not found in ${category}`);
  }
  const uploadHere = uploadHereFolders.next();

  // --- DUPLICATE CHECK ---
  // Check for duplicate filenames before distributing to workers.
  // If duplicates exist, halt the process and alert the admin.
  if (!AHA_CheckForDuplicateFiles3(uploadHere, category)) {
    updateSheet.getRange("A1").setValue("SCRIPT OFFLINE");
    return false; // Halt the import process
  }
  // --- END DUPLICATE CHECK ---

  // --- GCS UPLOAD FOR IBOT V2 ---
  // Upload files to GCS in parallel with v1 processing.
  // This allows iBot v2 to process the same files independently.
  // Failures here don't block v1 processing.
  try {
    const gcsResult = AHA_UploadFilesToGCS3(category, uploadHere);
    Logger.log(`GCS upload: ${gcsResult.uploaded} uploaded, ${gcsResult.failed} failed`);
  } catch (gcsError) {
    Logger.log(`GCS upload error (non-blocking): ${gcsError.message}`);
  }
  // --- END GCS UPLOAD ---

  const workerFolders = {};
  const numWorkers = 3;
  for (let i = 1; i <= numWorkers; i++) {
    const folderName = `Worker ${i}`;
    const matches = baseFolder.getFoldersByName(folderName);
    workerFolders[i] = matches.hasNext() ? matches.next() : baseFolder.createFolder(folderName);
  }

  // --- NEW RETRY LOOP LOGIC ---
  let totalFilesMoved = 0;
  let attempt = 0;
  const maxAttempts = 5; // Safety break to prevent infinite loops
  let filesWereFoundAtStart = false; // Flag to track if we ever found files

  while (attempt < maxAttempts) {
    attempt++;
    const files = [];
    const fileIterator = uploadHere.getFiles();
    while (fileIterator.hasNext()) {
      files.push(fileIterator.next());
    }

    const totalFiles = files.length;

    if (totalFiles === 0) {
      if (attempt === 1 && !filesWereFoundAtStart) {
        // This is the first run, and no files were found. This is a normal "no files" run.
        Logger.log("⚠️ No files found in Upload Here folder. Halting run.");
        AHA_SlackNotify3(`⚠️ No files found in 'Upload Here' for *${category}*. The process will now stop.`);
        updateSheet.getRange("A1").setValue("SCRIPT OFFLINE");
        return false; // Signal that no files were processed.
      } else {
        // Files were found on a previous loop, and now the folder is empty.
        Logger.log(`✅ All files successfully moved from Upload Here after ${attempt - 1} attempts.`);
        break; // Exit the while loop, success.
      }
    }
    
    // If we found files, set the flag so we know this was a "real" run
    filesWereFoundAtStart = true; 

    Logger.log(`🔍 [Attempt ${attempt}/${maxAttempts}] Found ${totalFiles} files. Moving them...`);

    files.sort((a, b) => a.getName().localeCompare(b.getName(), undefined, { numeric: true, sensitivity: 'base' }));

    const movedCounts = Array(numWorkers).fill(0);
    const filesPerWorker = Math.ceil(totalFiles / numWorkers);

    files.forEach((file, i) => {
      const targetWorkerIndex = Math.floor(i / filesPerWorker) + 1;
      try {
        file.moveTo(workerFolders[targetWorkerIndex]);
        movedCounts[targetWorkerIndex - 1]++;
      } catch (e) {
        Logger.log(`❌ Could not move "${file.getName()}" to Worker ${targetWorkerIndex}: ${e.message}`);
        AHA_SlackNotify3(`❌ Error moving "${file.getName()}" to Worker ${targetWorkerIndex}: ${e.message} <@U0A6B24777X>`);
      }
    });

    const movedThisAttempt = movedCounts.reduce((a, b) => a + b, 0);
    totalFilesMoved += movedThisAttempt;
    
    const summary = `✅ [Attempt ${attempt}] Split ${movedThisAttempt} files → W1: ${movedCounts[0]}, W2: ${movedCounts[1]}, W3: ${movedCounts[2]}`;
    Logger.log(summary);
    AHA_SlackNotify3(summary);

    // After moving, check if any files are left.
    // If not, the next loop iteration will find totalFiles = 0 and break.
    if (!uploadHere.getFiles().hasNext()) {
        Logger.log("✅ 'Upload Here' folder is now empty. Proceeding.");
        break; // Success, exit loop
    } else {
        Logger.log(`⚠️ Files are still present in 'Upload Here'. Waiting 30s and retrying...`);
        Utilities.sleep(30 * 1000); // Wait 30 seconds for Drive to catch up before next attempt
    }
  } // --- End of while loop ---

  // Check if the loop ended because it's still not empty
  if (attempt >= maxAttempts && uploadHere.getFiles().hasNext()) {
    Logger.log(`❌ FAILED: Files are still in 'Upload Here' after ${maxAttempts} attempts. Stopping process.`);
    AHA_SlackNotify3(`❌ *CRITICAL*: Files are still stuck in 'Upload Here' for *${category}* after ${maxAttempts} attempts. Manual check needed. <@U0A6B24777X>`);
    return false; // Return false because the split failed
  }
  
  // If we are here, it means the loop broke successfully.
  // We return true *only* if we actually found and moved files.
  return filesWereFoundAtStart;
}


/**
 * Triggers all worker web apps by fetching their URLs from the "Links List" sheet.
 * @param {GoogleAppsScript.Spreadsheet.Spreadsheet} ss The active spreadsheet object.
 * @param {string} category The target category to find worker URLs for.
 */
function AHA_TriggerAllWorkers3(ss, category) {
  const secretToken = 'CuanCengliCincai'; // This can also be moved to Script Properties for better security

  // --- MODIFICATION: Fetch Worker URLs dynamically from the "Links List" sheet ---
  const linksSheet = ss.getSheetByName("Links List");
  if (!linksSheet) {
    throw new Error("Sheet 'Links List' not found. Cannot trigger workers.");
  }

  const data = linksSheet.getDataRange().getValues();
  const headers = data.shift(); // Get and remove header row

  // Find column indices by their names to make the script robust
  const categoryCol = headers.indexOf("Category / Type");
  const roleCol = headers.indexOf("Role");
  const urlCol = headers.indexOf("Apps Script Deployment URL");

  if (categoryCol === -1 || roleCol === -1 || urlCol === -1) {
    throw new Error("Could not find required columns in 'Links List' sheet: 'Category / Type', 'Role', 'Apps Script Deployment URL'.");
  }

  const workerUrls = {};
  data.forEach(row => {
    const rowCategory = row[categoryCol];
    const rowRole = row[roleCol];
    const rowUrl = row[urlCol];

    // Check if the row matches the target category and is a worker role
    if (rowCategory === category && rowRole.startsWith("Worker") && rowUrl) {
      const workerNumber = rowRole.replace("Worker ", "").trim();
      if (workerNumber && !isNaN(workerNumber)) {
        workerUrls[workerNumber] = rowUrl;
      }
    }
  });

  if (Object.keys(workerUrls).length === 0) {
    throw new Error(`No worker URLs found for category "${category}" in 'Links List' sheet.`);
  }
  // --- END MODIFICATION ---

  const requests = Object.keys(workerUrls).map(workerId => ({
    url: workerUrls[workerId],
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify({
      command: 'StartWorker',
      token: secretToken
    }),
    muteHttpExceptions: true
  }));

  try {
    const responses = UrlFetchApp.fetchAll(requests);
    responses.forEach((response, i) => {
      const workerId = Object.keys(workerUrls)[i]; // Get the actual worker ID
      const message = `✅ ${category} Triggered Worker ${workerId}: ${response.getContentText()}`;
      Logger.log(message);
      AHA_SlackNotify3(message);
    });
  } catch (e) {
    const errorMessage = `❌ ${category} Error triggering workers: ${e.message}`;
    Logger.log(errorMessage);
    AHA_SlackNotify3(errorMessage);
  }
}


/**
 * Resets the import status of all workers for a specific category.
 * @param {GoogleAppsScript.Spreadsheet.Spreadsheet} ss The active spreadsheet object.
 * @param {string} targetCategory The category whose workers need resetting.
 * @param {string[]} workerNames The names of the workers to reset.
 */
function AHA_ResetWorkerCopyStatus(ss, targetCategory, workerNames = ["Worker 1", "Worker 2", "Worker 3"]) {
  const start = new Date();
  try {
    const controlSheet = ss.getSheetByName('System Dashboard');
    if (!controlSheet) {
      throw new Error('Sheet "System Dashboard" not found.');
    }

    // Assuming these constants are defined elsewhere in your project
    const DASHBOARD_START_ROW = 2; // Example value
    const DASHBOARD_CATEGORY_COL = 39; // Column AM (Example value)
    const DASHBOARD_STATUS_COL = 41; // Column AO (Example value)
    const DASHBOARD_CONTROL_ROWS = 100; // Example value

    // Get the relevant range that contains category, worker, and status
    const controlRange = controlSheet.getRange(
      DASHBOARD_START_ROW,
      DASHBOARD_CATEGORY_COL,
      DASHBOARD_CONTROL_ROWS,
      3 // Read Category, Worker, Status columns
    );
    const controlRangeData = controlRange.getValues();

    let updatedCount = 0;
    for (let i = 0; i < controlRangeData.length; i++) {
      const rowCategory = controlRangeData[i][0]; // Category column
      const rowWorker = controlRangeData[i][1];   // Worker column

      if (rowCategory === targetCategory && workerNames.includes(rowWorker)) {
        const targetRowInSheet = i + DASHBOARD_START_ROW;
        controlSheet.getRange(targetRowInSheet, DASHBOARD_STATUS_COL).setValue("Not Yet Done");
        updatedCount++;
        Logger.log(`Resetting status for ${rowCategory} - ${rowWorker} to "Not Yet Done".`);
      }
    }

    if (updatedCount > 0) {
      const msg = `✅ Reset status for ${updatedCount} workers in category *${targetCategory}* to "Not Yet Done".`;
      AHA_SlackNotify3(msg);
      Logger.log(msg);
    } else {
      const msg = `⚠️ No workers found for category *${targetCategory}* to reset.`;
      AHA_SlackNotify3(msg);
      Logger.log(msg);
    }

  } catch (error) {
    const fatalMsg = `❌ *Fatal Error* in ResetWorkerCopyStatus: ${error.message}`;
    Logger.log(fatalMsg);
    AHA_SlackNotify3(`${fatalMsg} <@U0A6B24777X>`);
  } finally {
    // Assuming AHA_LogRuntime3 is available globally
    // AHA_LogRuntime3(new Date() - start); 
  }
}
















