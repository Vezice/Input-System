////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Worker Management.gs
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
    AHA_SlackNotify3(`${errorMessage} <@U08TUF8LW2H>`); // Notify with specific error
  }
}


/**
 * Splits files from the 'Upload Here' folder into worker folders.
 * It now returns a boolean indicating if any files were processed.
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
    AHA_SlackNotify3(`${errorMsg} <@U08TUF8LW2H>`);
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

  const workerFolders = {};
  const numWorkers = 3;
  for (let i = 1; i <= numWorkers; i++) {
    const folderName = `Worker ${i}`;
    const matches = baseFolder.getFoldersByName(folderName);
    workerFolders[i] = matches.hasNext() ? matches.next() : baseFolder.createFolder(folderName);
  }

  const files = [];
  const fileIterator = uploadHere.getFiles();
  while (fileIterator.hasNext()) {
    files.push(fileIterator.next());
  }

  const totalFiles = files.length;
  
  // --- KEY CHANGE 3: Check for files and exit early if none are found ---
  if (totalFiles === 0) {
    Logger.log("⚠️ No files found in Upload Here folder. Halting run.");
    AHA_SlackNotify3(`⚠️ No files found in 'Upload Here' for *${category}*. The process will now stop.`);
    updateSheet.getRange("A1").setValue("SCRIPT OFFLINE"); // Set final status
    return false; // Signal that no files were processed.
  }
  
  Logger.log(`🔍 Found ${totalFiles} files in Upload Here for category ${category}`);

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
      AHA_SlackNotify3(`❌ Error moving "${file.getName()}" to Worker ${targetWorkerIndex}: ${e.message} <@U08TUF8LW2H>`);
    }
  });

  const movedTotal = movedCounts.reduce((a, b) => a + b, 0);
  const summary = `✅ [${category}] Split ${movedTotal} files → W1: ${movedCounts[0]}, W2: ${movedCounts[1]}, W3: ${movedCounts[2]}`;
  Logger.log(summary);
  AHA_SlackNotify3(summary);

  if (uploadHere.getFiles().hasNext()) {
    AHA_SlackNotify3(`⚠️ [${category}] Some files were not moved from Upload Here!`);
  }
  
  return true; // Signal that files were successfully processed.
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
    AHA_SlackNotify3(`${fatalMsg} <@U08TUF8LW2H>`);
  } finally {
    // Assuming AHA_LogRuntime3 is available globally
    // AHA_LogRuntime3(new Date() - start); 
  }
}
















