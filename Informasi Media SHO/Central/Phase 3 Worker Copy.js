////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Worker Copy.gs
//
// This script is fully dynamic and reads its configuration from script properties.
// To initialize, you must run the SETUP_SetAllSpreadsheetIDs function once.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// ------------------------------------------------------------------------------------------
// --- CONFIGURATION AND SETUP --------------------------------------------------------------
// ------------------------------------------------------------------------------------------

/**
 * ONE-TIME SETUP FUNCTION FOR A SPECIFIC CATEGORY
 *
 * This function reads the category name from 'System Dashboard'!E1. It then finds
 * the 'Central' and 'Worker' spreadsheet IDs for ONLY that category from the
 * 'Links List' sheet and saves them to the script's properties.
 *
 * Run this manually from the script editor whenever you change the target category.
 */
function AHA_SetConfigurationForCategory3() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  // 1. Get the target category from the System Dashboard
  const dashboardSheet = ss.getSheetByName("System Dashboard");
  if (!dashboardSheet) {
    throw new Error("A 'System Dashboard' sheet is required.");
  }
  const targetCategory = dashboardSheet.getRange("E1").getValue();
  if (!targetCategory) {
    throw new Error("Please set a category name in cell E1 of the 'System Dashboard' sheet.");
  }
  Logger.log(`Setting configuration for category: "${targetCategory}"`);

  // 2. Access the Links List sheet
  const linksSheet = ss.getSheetByName("Links List");
  if (!linksSheet) {
    throw new Error("The active spreadsheet does not have a 'Links List' sheet.");
  }

  const data = linksSheet.getDataRange().getValues();
  const headers = data.shift(); // Get and remove header row

  // 3. Find the required column indices
  const categoryCol = headers.indexOf("Category / Type");
  const roleCol = headers.indexOf("Role");
  const idCol = headers.indexOf("Spreadsheet ID");
  const slackCol = headers.indexOf("Slack Webhook URL");

  if (categoryCol === -1 || roleCol === -1 || idCol === -1 || slackCol === -1) {
    throw new Error("Could not find 'Category / Type', 'Role', 'Spreadsheet ID', and 'Slack Webhook URL' columns in 'Links List'.");
  }

  // 4. Clear all old properties to prevent conflicts
  const properties = PropertiesService.getScriptProperties();

  let foundCentral = false;
  let workersFound = 0;

  // 5. Loop through the list and save properties ONLY for the target category
  for (const row of data) {
    const rowCategory = row[categoryCol];
    const role = row[roleCol];
    const id = row[idCol];
    const slackURL = row[slackCol];

    // --- The Key Change: Only process rows that match the target category ---
    if (rowCategory === targetCategory) {
      if (role && id && slackURL) {
        if (role === "Central") {
          properties.setProperty('CENTRAL_SPREADSHEET_ID', id);
          properties.setProperty('SLACK_WEBHOOK_URL', slackURL);
          foundCentral = true;
        } else if (role.startsWith("Worker")) {
          properties.setProperty(role, id); // e.g., key='Worker 1', value='...'
          workersFound++;
        }
      }
    }
  }

  // 6. Provide a final confirmation
  if (!foundCentral) {
    throw new Error(`Could not find a 'Central' role for the category "${targetCategory}" in your 'Links List'.`);
  }
  if (workersFound === 0) {
      Logger.log(`Warning: No workers were found for the category "${targetCategory}".`)
  }

  // const ui = SpreadsheetApp.getUi();
  // ui.alert('✅ Success!', `Configuration for category "${targetCategory}" has been set.\n\nCentral ID Found: ${foundCentral}\nWorker IDs Found: ${workersFound}`, ui.ButtonSet.OK);
  Logger.log(`Configuration set. Central: ${foundCentral}, Workers: ${workersFound}`);
}


// ------------------------------------------------------------------------------------------
// --- MAIN OPERATIONAL SCRIPT --------------------------------------------------------------
// ------------------------------------------------------------------------------------------

// Constants for System Dashboard sheet columns (using 1-based indexing for getRange)
const DASHBOARD_START_ROW = 5;
const DASHBOARD_CATEGORY_COL = 39; // Column AM
const DASHBOARD_WORKER_COL = 40;   // Column AN
const DASHBOARD_STATUS_COL = 41;   // Column AO
const DASHBOARD_CONTROL_ROWS = 3;  // Number of rows for Worker 1, 2, 3
// --- CENTRAL CONFIGURATION ---
const CONFIG = {
  FAILURE_LOG_DOC_ID: "1sEqcKalRpWOakM82Ffr-BRNN17ft4wUnv31fSIuQZYw", // <--- THIS WAS MISSING
};

/**
 * Web App entry point for the Central Sheet.
 * Handles requests forwarded from the Admin Sheet (originally from Slack)
 * and notifications from its own internal workers.
 *
 * @param {Object} e The event parameter from the web app request.
 */
function doPost(e) {
  const start = new Date();
  const lock = LockService.getScriptLock();
  const LOCK_TIMEOUT_MS = 30000; // 30 seconds

  // --- PING COMMAND HANDLER (for /ibot pingall) ---
  // Check if this is a JSON ping request
  if (e.postData && e.postData.type === "application/json") {
    try {
      const jsonData = JSON.parse(e.postData.contents);
      if (jsonData.command === "ping") {
        // Refresh configuration (including Slack webhook URL) from Links List
        AHA_SetConfigurationForCategory3();
        AHA_SlackNotify3(`🟢 Online`);
        return ContentService.createTextOutput("PONG").setMimeType(ContentService.MimeType.TEXT);
      }
    } catch (jsonErr) {
      // Not valid JSON, continue to other handlers
    }
  }

  // Check for a Slack-specific parameter. This will be present in any
  // request forwarded from the Admin Sheet.
  if (e.parameter.user_name) {
    // --- This block handles the forwarded Slack command ---

    // Since the Admin Sheet already handled the initial 3-second Slack timeout,
    // this part can now run synchronously without causing an error.
    // The UrlFetchApp from the Admin sheet will wait for this to complete.
    try {
      AHA_HandleSlackCommand(e);

      // Return a clear success message. This goes back to the Admin sheet's
      // UrlFetchApp call and is useful for logging. It is NOT sent to Slack.
      return ContentService.createTextOutput("SUCCESS: Slack command processed.").setMimeType(ContentService.MimeType.TEXT);
    } catch (error) {
       Logger.log(`Error in Central doPost (Slack Command): ${error.message}`);
       // Return an error message for logging purposes on the Admin sheet side.
       return ContentService.createTextOutput(`ERROR: ${error.message}`).setMimeType(ContentService.MimeType.TEXT);
    }

  } else {
    // --- This block handles internal worker notifications ---

    try {
      if (!lock.tryLock(LOCK_TIMEOUT_MS)) {
        Logger.log("doPost: Could not acquire lock. Another instance is running.");
        AHA_SlackNotify3(`⚠️ *Central Sheet Busy*: Lock acquisition failed.`);
        return ContentService.createTextOutput("SERVER_BUSY").setMimeType(ContentService.MimeType.TEXT);
      }
      
      const response = AHA_HandleWorkerNotification(e);
      return response;

    } catch (error) {
      Logger.log(`Error in Central doPost (Worker): ${error.message}`);
      AHA_SlackNotify3(`❌ *Fatal Error in doPost*: ${error.message} <@U0A6B24777X>`);
      return ContentService.createTextOutput('ERROR: ' + error.message).setMimeType(ContentService.MimeType.TEXT);
    } finally {
      if (lock.hasLock()) {
        lock.releaseLock();
      }
      const end = new Date();
      AHA_LogRuntime3(end - start);
    }
  }
}

/**
 * Handles incoming requests from a worker spreadsheet, parsing JSON data.
 * @param {Object} e - The event object from a doPost call.
 * @returns {ContentService.TextOutput}
 */
function AHA_HandleWorkerNotification(e) {
  const centralId = PropertiesService.getScriptProperties().getProperty('CENTRAL_SPREADSHEET_ID');
  if (!centralId) {
    const errorMsg = "FATAL ERROR: Central Spreadsheet ID not found in script properties. Please run the setup function.";
    Logger.log(errorMsg);
    AHA_SlackNotify3(errorMsg); 
    return ContentService.createTextOutput(errorMsg).setMimeType(ContentService.MimeType.TEXT);
  }

  // --- KEY CHANGE IS HERE ---
  // The entire JSON payload is in e.postData.contents. We must parse it.
  let data;
  try {
    data = JSON.parse(e.postData.contents);
  } catch (jsonError) {
    AHA_SlackNotify3(`❌ *Error in doPost*: Failed to parse JSON from worker. Error: ${jsonError.message}`);
    return ContentService.createTextOutput('ERROR: Invalid JSON format received.').setMimeType(ContentService.MimeType.TEXT);
  }

  // Now, access properties from the parsed data object.
  const category = data.category;
  const worker = data.worker;
  // --- END OF KEY CHANGE ---

  if (!category || !worker) {
    AHA_SlackNotify3(`❌ *Error in doPost*: Missing 'category' or 'worker' in JSON payload.`);
    return ContentService.createTextOutput('ERROR: Missing parameters (category or worker)').setMimeType(ContentService.MimeType.TEXT);
  }

  const ss = SpreadsheetApp.openById(centralId);
  const dashboardSheet = ss.getSheetByName('System Dashboard');
  if (!dashboardSheet) {
    AHA_SlackNotify3(`❌ *Error in doPost*: 'System Dashboard' sheet not found.`);
    return ContentService.createTextOutput('ERROR: "System Dashboard" sheet not found.').setMimeType(ContentService.MimeType.TEXT);
  }
  
  // Re-use the constants from your original script
  const DASHBOARD_START_ROW = 5;
  const DASHBOARD_CATEGORY_COL = 39;
  const DASHBOARD_WORKER_COL = 40;
  const DASHBOARD_STATUS_COL = 41;
  const DASHBOARD_CONTROL_ROWS = 3;

  const workerStatusRange = dashboardSheet.getRange(DASHBOARD_START_ROW, DASHBOARD_CATEGORY_COL, DASHBOARD_CONTROL_ROWS, 3).getValues();
  let targetRowIndexInSheet = -1;
  for (let i = 0; i < workerStatusRange.length; i++) {
    if (workerStatusRange[i][0] === category && workerStatusRange[i][1] === worker) {
      targetRowIndexInSheet = i + DASHBOARD_START_ROW;
      break;
    }
  }

  if (targetRowIndexInSheet === -1) {
    AHA_SlackNotify3(`❌ *Error in doPost*: Category/Worker not found in dashboard.`);
    return ContentService.createTextOutput(`ERROR: Category/Worker not found in dashboard.`).setMimeType(ContentService.MimeType.TEXT);
  }

  dashboardSheet.getRange(targetRowIndexInSheet, DASHBOARD_STATUS_COL).setValue('Done');
  Logger.log(`Updated status for Category: ${category}, Worker: ${worker} to 'Done'.`);

  const updatedControlRange = dashboardSheet.getRange(DASHBOARD_START_ROW, DASHBOARD_CATEGORY_COL, DASHBOARD_CONTROL_ROWS, 3).getValues();
  let allWorkersDoneForCategory = true;
  for (let i = 0; i < updatedControlRange.length; i++) {
    if (updatedControlRange[i][0] === category && updatedControlRange[i][2] !== 'Done') {
      allWorkersDoneForCategory = false;
      break;
    }
  }

  if (allWorkersDoneForCategory) {
    Logger.log(`All workers for category '${category}' are done. Checking merge type.`);
    const properties = PropertiesService.getScriptProperties();
    properties.setProperties({
      'merge_category': category,
      'merge_worker_index': '0'
    });

    if (category.startsWith("BA Dash ")) {
      Logger.log(`Category '${category}' is a BA Dash type. Initiating APPEND-merge trigger.`);
      AHA_SlackNotify3(`📈 *BA Dash Merge*: Initiating APPEND-merge for *${category}*.`);
      AHA_DeleteTriggerByName3('AHA_ProcessBADashMergeBatch3');
      ScriptApp.newTrigger('AHA_ProcessBADashMergeBatch3').timeBased().after(10 * 1000).create();
    } else {
      Logger.log(`Category '${category}' is a Standard type. Initiating REPLACE-merge trigger.`);
      AHA_SlackNotify3(`🎉 *Standard Merge*: Initiating REPLACE-merge for category *${category}*.`);
      AHA_DeleteTriggerByName3('AHA_ProcessMergeBatch3');
      ScriptApp.newTrigger('AHA_ProcessMergeBatch3').timeBased().after(10 * 1000).create();
    }
  } else {
    Logger.log(`⏳ Workers Pending: Not all workers for ${category} are done yet.`);
  }
  return ContentService.createTextOutput('OK').setMimeType(ContentService.MimeType.TEXT);
}


/**
 * Handles the initial Slack command, logs it, acknowledges it, and
 * -- MODIFIED: Creates a 5-minute trigger to start the workers --
 *
 * @param {Object} e The event parameter from the doPost request, containing Slack data.
 */
function AHA_HandleSlackCommand(e) {
  // --- CONFIGURATION ---
  const logSheetName = "Command History Log";
  // --------------------

  const user = e.parameter.user_name || "Unknown User";
  const commandParameter = (e.parameter.text || "").trim();

  // --- 1. Log the command details to the spreadsheet ---
  try {
    const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(logSheetName);
    if (sheet) {
      const timestamp = new Date();
      const fullCommand = `/ibot ${commandParameter}`;
      const source = "Slack";
      
      // Create the row of data to be logged
      const logData = [
        timestamp,         // Column A: Time
        user,              // Column B: User
        fullCommand,       // Column C: Command
        commandParameter,  // Column D: Parameter
        source             // Column E: Source
      ];
      
      // Append the new row to the log sheet
      sheet.appendRow(logData);

    } else {
      Logger.log(`Warning: Log sheet named "${logSheetName}" was not found.`);
    }
  } catch (logError) {
    Logger.log(`Error while trying to log command: ${logError.message}`);
  }

  // --- 2. Acknowledge the command in the Slack channel ---
  // --- MODIFICATION: Updated the text to include the 5-minute wait ---
  // --- SLACK DISABLED: Only send if response_url is provided ---
  const responseUrl = e.parameter.response_url;
  if (responseUrl && responseUrl.trim() !== "") {
    const ackText = `👀 Processing your request, ${user}... *Adding a 5-minute delay* to allow Google Drive to sync. File processing will begin shortly.`;
    const slackPayload = {
      method: "post",
      contentType: "application/json",
      payload: JSON.stringify({ text: ackText })
    };
    UrlFetchApp.fetch(responseUrl, slackPayload);
  } else {
    Logger.log("No response_url provided - skipping Slack acknowledgment");
  }
  // --- END MODIFICATION ---


  // --- 3. Start the main worker process via a trigger ---
  // --- MODIFICATION: Replaced direct call with a one-time trigger ---
  Logger.log("Creating 5-minute trigger for AHA_TriggeredStartWorkers");
  
  // Delete any old triggers first to be safe
  AHA_DeleteTriggerByName3('AHA_TriggeredStartWorkers'); 
  
  ScriptApp.newTrigger('AHA_TriggeredStartWorkers')
    .timeBased()
    .after(5 * 60 * 1000) // 5 minutes
    .create();
  // --- END MODIFICATION ---
}

/**
 * --- NEW FUNCTION ---
 * This function is called by the 5-minute one-time trigger
 * created by AHA_HandleSlackCommand. Its only job is to
 * start the main worker process.
 */
function AHA_TriggeredStartWorkers() {
  try {
    Logger.log("5-minute delay complete. Calling AHA_StartWorkers3...");
    AHA_StartWorkers3(); // This is the original function
  } catch(e) {
    Logger.log(`❌ Error in AHA_TriggeredStartWorkers: ${e.message}`);
    AHA_SlackNotify3(`❌ *Fatal Error*: The 5-minute trigger failed to start the workers: ${e.message} <@U0A6B24777X>`);
  }
}


/**
 * Processes one worker's data at a time for a STANDARD (replace) merge.
 */
function AHA_ProcessMergeBatch3() {
  const properties = PropertiesService.getScriptProperties();
  const category = properties.getProperty('merge_category');
  const workerIndexStr = properties.getProperty('merge_worker_index');
  const centralId = properties.getProperty('CENTRAL_SPREADSHEET_ID');

  if (!centralId) {
    Logger.log("FATAL ERROR: Central Spreadsheet ID not found. Stopping merge.");
    AHA_DeleteTriggerByName3('AHA_ProcessMergeBatch3');
    return;
  }
  if (!category || !workerIndexStr) {
    Logger.log("Merge process triggered without necessary properties. Stopping.");
    AHA_DeleteTriggerByName3('AHA_ProcessMergeBatch3');
    return;
  }

  const workerIndex = parseInt(workerIndexStr, 10);
  const targetSS = SpreadsheetApp.openById(centralId);
  const controlSheet = targetSS.getSheetByName('System Dashboard');
  
  if (!controlSheet) {
    Logger.log(`ERROR: 'System Dashboard' sheet not found. Stopping merge.`);
    AHA_SlackNotify3(`❌ *Merge Process Error*: 'System Dashboard' sheet not found. <@U0A6B24777X>`);
    AHA_DeleteTriggerByName3('AHA_ProcessMergeBatch3');
    return;
  }

  const controlData = controlSheet.getRange(DASHBOARD_START_ROW, DASHBOARD_CATEGORY_COL, DASHBOARD_CONTROL_ROWS, 3).getValues();
  const workersToProcess = controlData.filter(row => row[0] === category).map(row => row[1]);
  const tempSheetName = `Temp ${category}`;
  let tempSheet;

  if (workerIndex === 0) {
    let existingTempSheet = targetSS.getSheetByName(tempSheetName);
    if (existingTempSheet) {
      targetSS.deleteSheet(existingTempSheet);
    }
    tempSheet = targetSS.insertSheet(tempSheetName);
    tempSheet.getRange(1, 1, tempSheet.getMaxRows(), tempSheet.getMaxColumns()).setNumberFormat("@");
    Logger.log(`Created new temporary sheet: ${tempSheetName}`);
  } else {
    tempSheet = targetSS.getSheetByName(tempSheetName);
    if (!tempSheet) {
      Logger.log(`ERROR: Temp sheet '${tempSheetName}' not found. Stopping.`);
      AHA_SlackNotify3(`❌ *Merge Process Error*: Failed to create temporary sheet: ${tempSheetName}. <@U0A6B24777X>`);
      AHA_DeleteTriggerByName3('AHA_ProcessMergeBatch3');
      return;
    }
  }

  const worker = workersToProcess[workerIndex];
  const sourceId = properties.getProperty(worker); // Dynamic lookup, e.g., getProperty('Worker 1')
  
  if (!sourceId) {
    Logger.log(`ERROR: Source ID for worker '${worker}' not found in properties. Stopping merge.`);
    AHA_SlackNotify3(`❌ *Merge Process Error*: Source ID for worker '${worker}' not found in properties. Stopping merge. <@U0A6B24777X>`);
    AHA_DeleteTriggerByName3('AHA_ProcessMergeBatch3');
    return;
  }
  const sourceSS = SpreadsheetApp.openById(sourceId);
  const sourceSheet = sourceSS.getSheetByName(category);
  
  if (!sourceSheet) {
    Logger.log(`ERROR: Source sheet '${category}' not found in worker '${worker}'. Stopping merge.`);
    AHA_SlackNotify3(`❌ *Merge Process Error*: Temporary sheet '${tempSheetName}' not found. Stopping. <@U0A6B24777X>`);
    AHA_DeleteTriggerByName3('AHA_ProcessMergeBatch3');
    return;
  }
  
  const dataToCopy = sourceSheet.getDataRange().getDisplayValues();

  if (dataToCopy && dataToCopy.length > 0) {
    if (workerIndex === 0 && tempSheet) {
      const headers = [dataToCopy[0]];
      tempSheet.getRange(1, 1, 1, headers[0].length).setValues(headers).setFontWeight("bold").setBackground("yellow");

      // Immediately delete unused columns to prevent cell limit issues
      AHA_TrimUnusedColumns3(tempSheet, headers[0].length);
    }
    const rowsToCopy = dataToCopy.slice(1);
    if (rowsToCopy.length > 0 && tempSheet) { 
      // Define the target range
      const targetRange = tempSheet.getRange(tempSheet.getLastRow() + 1, 1, rowsToCopy.length, rowsToCopy[0].length);
      
      // Force Plain Text (@) formatting BEFORE pasting to preserve "04 December 2025" as-is
      targetRange.setNumberFormat("@").setValues(rowsToCopy);

      Logger.log(`Copied ${rowsToCopy.length} rows from ${worker} for ${category}.`);
    }
  } else {
    Logger.log(`${worker} for ${category} had no data to copy.`);
  }

  if (workerIndex < workersToProcess.length - 1) {
    properties.setProperty('merge_worker_index', (workerIndex + 1).toString());
    AHA_DeleteTriggerByName3('AHA_ProcessMergeBatch3');
    ScriptApp.newTrigger('AHA_ProcessMergeBatch3').timeBased().after(10 * 1000).create();
  } else {
    Logger.log(`All workers for '${category}' have been merged. Finalizing.`);
    AHA_FinalizeMerge3(category, tempSheetName);
    properties.deleteProperty('merge_category');
    properties.deleteProperty('merge_worker_index');
    AHA_DeleteTriggerByName3('AHA_ProcessMergeBatch3');
  }
}

/**
 * Processes a merge for "BA Dash" categories by APPENDING new data.
 */
function AHA_ProcessBADashMergeBatch3() {
  const properties = PropertiesService.getScriptProperties();
  const category = properties.getProperty('merge_category');
  const workerIndexStr = properties.getProperty('merge_worker_index');
  const centralId = properties.getProperty('CENTRAL_SPREADSHEET_ID');

  if (!centralId) {
    Logger.log("FATAL ERROR: Central Spreadsheet ID not found. Stopping BA Dash merge.");
    AHA_SlackNotify3(`❌ *BA Dash Merge Error*: Central Spreadsheet ID not found. Stopping BA Dash merge. <@U0A6B24777X>`);
    AHA_DeleteTriggerByName3('AHA_ProcessBADashMergeBatch3');
    return;
  }
  if (!category || !workerIndexStr) {
    Logger.log("BA Dash Merge triggered without properties. Stopping.");
    AHA_SlackNotify3(`❌ *BA Dash Merge Error*: BA Dash Merge triggered without properties. Stopping. <@U0A6B24777X>`);
    AHA_DeleteTriggerByName3('AHA_ProcessBADashMergeBatch3');
    return;
  }

  const workerIndex = parseInt(workerIndexStr, 10);
  const targetSS = SpreadsheetApp.openById(centralId);
  const controlSheet = targetSS.getSheetByName('System Dashboard');
  
  if (!controlSheet) {
    Logger.log(`ERROR: 'System Dashboard' sheet not found. Stopping BA Dash merge.`);
    AHA_SlackNotify3(`❌ *BA Dash Merge Error*: 'System Dashboard' sheet not found. <@U0A6B24777X>`);
    AHA_DeleteTriggerByName3('AHA_ProcessBADashMergeBatch3');
    return;
  }

  const controlData = controlSheet.getRange(DASHBOARD_START_ROW, DASHBOARD_CATEGORY_COL, DASHBOARD_CONTROL_ROWS, 3).getValues();
  const workersToProcess = controlData.filter(row => row[0] === category).map(row => row[1]);
  const tempSheetName = `Temp ${category}`;
  let tempSheet;

  if (workerIndex === 0) {
    let existingTempSheet = targetSS.getSheetByName(tempSheetName);
    if (existingTempSheet) {
      targetSS.deleteSheet(existingTempSheet);
    }
    tempSheet = targetSS.insertSheet(tempSheetName);
    tempSheet.getRange(1, 1, tempSheet.getMaxRows(), tempSheet.getMaxColumns()).setNumberFormat("@");
    Logger.log(`Created new temp sheet: ${tempSheetName}`);

    const originalSheet = targetSS.getSheetByName(category);
    if (originalSheet) {
      // Use getDisplayValues() to capture the exact look (e.g., "04 December 2025")
      const originalData = originalSheet.getDataRange().getDisplayValues();
      if (originalData.length > 0) {
          const targetRange = tempSheet.getRange(1, 1, originalData.length, originalData[0].length);
          // Force Plain Text formatting to prevent auto-conversion
          targetRange.setNumberFormat("@").setValues(originalData);

          // Immediately delete unused columns to prevent cell limit issues
          AHA_TrimUnusedColumns3(tempSheet, originalData[0].length);

          Logger.log(`Copied ${originalData.length} existing rows from '${category}'.`);
      }
    }
  } else {
    tempSheet = targetSS.getSheetByName(tempSheetName);
    if (!tempSheet) {
      Logger.log(`ERROR: Temp sheet '${tempSheetName}' not found for BA Dash merge. Stopping.`);
      AHA_SlackNotify3(`❌ *BA Dash Merge Error*: Temp sheet '${tempSheetName}' not found. <@U0A6B24777X>`);
      AHA_DeleteTriggerByName3('AHA_ProcessBADashMergeBatch3');
      return;
    }
  }

  const worker = workersToProcess[workerIndex];
  const sourceId = properties.getProperty(worker); // Dynamic lookup
  
  if (!sourceId) {
    Logger.log(`ERROR: Source ID for worker '${worker}' not found. Stopping BA Dash merge.`);
    AHA_SlackNotify3(`❌ *BA Dash Merge Error*: Source ID for '${worker}' not found. <@U0A6B24777X>`);
    AHA_DeleteTriggerByName3('AHA_ProcessBADashMergeBatch3');
    return;
  }
  
  const sourceSS = SpreadsheetApp.openById(sourceId);
  const sourceSheet = sourceSS.getSheetByName(category);
  
  if (sourceSheet) {
    const dataToCopy = sourceSheet.getDataRange().getDisplayValues();
    if (dataToCopy && dataToCopy.length > 1) {
      const rowsToCopy = dataToCopy.slice(1); 
      if (rowsToCopy.length > 0) {
        const targetRange = tempSheet.getRange(tempSheet.getLastRow() + 1, 1, rowsToCopy.length, rowsToCopy[0].length);
        // Force Plain Text formatting before pasting
        targetRange.setNumberFormat("@").setValues(rowsToCopy);

        Logger.log(`Appended ${rowsToCopy.length} rows from ${worker} for ${category}.`);
      }
    } else {
      Logger.log(`${worker} for ${category} had no data rows to append.`);
    }
  } else {
    Logger.log(`Source sheet '${category}' not found in worker '${worker}'. Skipping.`);
  }

  if (workerIndex < workersToProcess.length - 1) {
    properties.setProperty('merge_worker_index', (workerIndex + 1).toString());
    AHA_DeleteTriggerByName3('AHA_ProcessBADashMergeBatch3');
    ScriptApp.newTrigger('AHA_ProcessBADashMergeBatch3').timeBased().after(10 * 1000).create();
  } else {
    Logger.log(`All workers for BA Dash category '${category}' have been processed. Finalizing.`);
    AHA_FinalizeMerge3(category, tempSheetName);
    properties.deleteProperty('merge_category');
    properties.deleteProperty('merge_worker_index');
    AHA_DeleteTriggerByName3('AHA_ProcessBADashMergeBatch3');
  }
}

/**
 * Finalizes the merge by replacing the old sheet with the new temp sheet.
 */
function AHA_FinalizeMerge3(category, tempSheetName) {
  const centralId = PropertiesService.getScriptProperties().getProperty('CENTRAL_SPREADSHEET_ID');
  if (!centralId) {
    Logger.log("FATAL ERROR: Central Spreadsheet ID not found in FinalizeMerge.");
    return;
  }
  const targetSS = SpreadsheetApp.openById(centralId);
  const tempSheet = targetSS.getSheetByName(tempSheetName);

  if (!tempSheet) {
      Logger.log(`ERROR: Temp sheet '${tempSheetName}' not found during finalization. Cannot complete merge.`);
      return;
  }

  const oldSheet = targetSS.getSheetByName(category);
  if (oldSheet) {
    targetSS.deleteSheet(oldSheet);
    Logger.log(`Deleted old category sheet: ${category}`);
  }
  tempSheet.setName(category);
  Logger.log(`Renamed temporary sheet to '${category}'. Merge complete.`);
  AHA_SlackNotify3(`✅ *Merge Complete*: Merged data for category *${category}* successfully!`);

  const controlSheet = targetSS.getSheetByName('System Dashboard');
  const controlData = controlSheet.getRange(DASHBOARD_START_ROW, DASHBOARD_CATEGORY_COL, DASHBOARD_CONTROL_ROWS, 3).getValues();
  for (let i = 0; i < controlData.length; i++) {
    if (controlData[i][0] === category) {
      const originalRowIndex = i + DASHBOARD_START_ROW;
      controlSheet.getRange(originalRowIndex, DASHBOARD_STATUS_COL).setValue('Imported');
    }
  }
  Logger.log(`Updated dashboard statuses for '${category}' to 'Imported'.`);
  
  controlSheet.getRange("A1").setValue("SCRIPT OFFLINE");

  // Export to BigQuery (non-blocking, controlled by BIGQUERY_ENABLED property)
  try {
    AHA_ExportToBigQuery3(category);
  } catch (bqError) {
    Logger.log(`BigQuery export failed (non-fatal): ${bqError.message}`);
  }

  // Run brand validation and report results to Slack
  try {
    AHA_ValidateCategoryBrands3(category);
  } catch (valError) {
    Logger.log(`Brand validation failed (non-fatal): ${valError.message}`);
  }

  AHA_CheckFailedImports3();
}

/**
 * Helper function to delete triggers by their handler function name.
 */
function AHA_DeleteTriggerByName3(triggerFunctionName) {
  const centralId = PropertiesService.getScriptProperties().getProperty('CENTRAL_SPREADSHEET_ID');
  if (!centralId) {
    Logger.log("Cannot delete trigger. Central ID is not set in properties.");
    return;
  }
  
  ScriptApp.getProjectTriggers().forEach(trigger => {
    if (trigger.getHandlerFunction() === triggerFunctionName) {
      ScriptApp.deleteTrigger(trigger);
    }
  });
}

/**
 * Checks the central 'Failed' folder, reads the 'FAILURE_LOG' Google SPREADSHEET,
 * and sends a smart, cross-referenced report to Slack.
 *
 * -- MODIFIED to read from a Google Sheet, not a Google Doc --
 */
function AHA_CheckFailedImports3() {
  // --- CONFIGURATION ---
  // This is the ID of your FAILURE LOG SPREADSHEET
  const FAILURE_LOG_DOC_ID = "1sEqcKalRpWOakM82Ffr-BRNN17ft4wUnv31fSIuQZYw"; // This is now a Spreadsheet ID
  const MENTION_USER_ON_ERROR = "<@U0A6B24777X>";
  // ---
  
  try {
    const moveFolderId = '1zQEPDi4dI3gUJiEKYp-GQI8lfNFwL1sh'; // Base 'Move'
    const moveFolder = DriveApp.getFolderById(moveFolderId);
    const parents = moveFolder.getParents();
    if (!parents.hasNext()) {
        throw new Error(`The 'Move' folder does not have a parent folder.`);
    }
    const rootFolder = parents.next();
    const failedFolders = rootFolder.getFoldersByName('Failed');

    if (!failedFolders.hasNext()) {
      AHA_SlackNotify3(`✅ No central 'Failed' folder was found. Nothing to report.`);
      return;
    }

    const failedFolder = failedFolders.next();
    const files = failedFolder.getFiles();
    const filesInFolder = new Map();
    while (files.hasNext()) {
      const file = files.next();
      const fileName = file.getName();
      // Exclude FAILURE_LOG from the count - it's a permanent log file, not a failed import
      if (fileName === "FAILURE_LOG") {
        continue;
      }
      filesInFolder.set(fileName, file.getDateCreated());
    }

    if (filesInFolder.size === 0) {
      AHA_SlackNotify3(`✅ Import process complete! No failed files detected.`);
      return;
    }

    // --- FIX: Read and Parse the Google SPREADSHEET ---
    const failureLog = new Map();
    try {
      // 1. Open the SPREADSHEET, not a Doc
      const logSpreadsheet = SpreadsheetApp.openById(FAILURE_LOG_DOC_ID);
      // 2. Get the first sheet (or change "getSheets()[0]" to "getSheetByName('LogSheetName')")
      const logSheet = logSpreadsheet.getSheets()[0]; 
      if (logSheet.getLastRow() < 2) {
        throw new Error("Failure log sheet is empty.");
      }
      
      // 3. Get all data from the sheet (assuming headers are in row 1)
      const logLines = logSheet.getRange(2, 1, logSheet.getLastRow() - 1, 4).getValues();
      
      // 4. Loop through the ROWS (which are arrays)
      for (const row of logLines) {
        if (!row || row.length < 4) continue; // Skip malformed rows

        // Assign columns directly. Assumes:
        // Col A: Timestamp, Col B: File Name, Col C: Reason, Col D: Worker
        const logDate = new Date(row[0]); // getValues() returns a Date object
        const fileName = row[1].toString().trim();
        const reason = row[2].toString().trim();
        const workerInfo = row[3].toString().trim();

        if (!fileName || isNaN(logDate.getTime())) continue; // Skip rows with invalid data

        // Check if this file is one of the ones *currently* in the folder
        if (filesInFolder.has(fileName)) {
          const existingEntry = failureLog.get(fileName);
          // Only store the MOST RECENT log entry for this file
          if (!existingEntry || logDate > existingEntry.date) {
            failureLog.set(fileName, {
              date: logDate,
              reason: reason,
              worker: workerInfo
            });
          }
        }
      }
    } catch (docErr) {
      // This error will now correctly report issues with SpreadsheetApp
      AHA_SlackNotify3(`❌ *Error*: Could not read the Failure Log Sheet: ${docErr.message} ${MENTION_USER_ON_ERROR}`);
      // Continue without log data if it fails
    }
    // --- END OF FIX ---

    // --- Build the Smart Slack Message ---
    // Tag admin since there are actual failed files that need attention
    let message = `🚨 *Failed Imports Report!* ${filesInFolder.size} file(s) are in the 'Failed' folder: ${MENTION_USER_ON_ERROR}\n\n`;
    const filesWithLogs = [];
    const filesWithoutLogs = [];

    for (const [fileName, dateCreated] of filesInFolder.entries()) {
      const logEntry = failureLog.get(fileName);
      if (logEntry) {
        // We found a matching log entry
        filesWithLogs.push(
          `• *${fileName}*` +
          `\n   *Reason:* ${logEntry.reason}` +
          `\n   *Source:* ${logEntry.worker}` +
          `\n   *(Logged: ${logEntry.date.toLocaleString()})*`
        );
      } else {
        // No log found for this file. It was likely moved manually.
        filesWithoutLogs.push(
          `• *${fileName}*` +
          `\n   *(No failure log found. Added to folder: ${dateCreated.toLocaleString()})*`
        );
      }
    }

    if (filesWithLogs.length > 0) {
      message += "--- *Files with Logged Reason* ---\n" + filesWithLogs.join("\n");
    }
    if (filesWithoutLogs.length > 0) {
      message += "\n\n--- *Files with No Log* (Manual/Old) ---\n" + filesWithoutLogs.join("\n");
    }

    AHA_SlackNotify3(message);

  } catch (e) {
    Logger.log(`Fatal Error in AHA_CheckFailedImports: ${e.message}`);
    AHA_SlackNotify3(`❌ *Fatal Error* in CheckFailedImports: ${e.message} ${MENTION_USER_ON_ERROR}`);
  }
  
  // This function should still run at the end
  AHA_FreezeImportStatus3();
}

/**
 * Finds the current import status, copies it, and appends it to the
 * permanent historical log. This function should be called once at the
 * very end of a successful import process.
 */
function AHA_FreezeImportStatus3() {
  // --- CONFIGURATION ---
  const sheetName = "Import Status History Log";
  const sourceRange = "S3:V3"; // The "Current Status" row to copy
  const destinationColumn = "X"; // The first column of the historical log
  // --------------------

  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = ss.getSheetByName(sheetName);

    if (!sheet) {
      Logger.log(`Error: Sheet named "${sheetName}" was not found.`);
      // Depending on your needs, you might want to send a Slack notification here.
      return;
    }

    // 1. Get the values from the source "Current Status" range.
    // We use getValues() which returns a 2D array, so we take the first row.
    const sourceData = sheet.getRange(sourceRange).getValues()[0];

    // 2. Find the last row with content in the destination column.
    // This tells us where to paste the new historical record.
    const lastRow = sheet.getRange(destinationColumn + "1:" + destinationColumn + sheet.getMaxRows())
                         .getValues()
                         .filter(String)
                         .length;
    
    // The next empty row will be the last row with content + 1.
    const destinationRowIndex = lastRow + 1;

    // 3. Write the copied data to the new row in the historical log section.
    // The destination range is constructed dynamically.
    // e.g., sheet.getRange("X3:AA3")
    sheet.getRange(destinationRowIndex, 24, 1, 4).setValues([sourceData]);
    
    Logger.log(`Successfully froze import status to row ${destinationRowIndex} in '${sheetName}'.`);

  } catch (e) {
    Logger.log(`A fatal error occurred in AHA_FreezeImportStatus: ${e.message}`);
    // Consider adding a Slack notification for errors.
    // AHA_SlackNotify3(`❌ Error freezing import status: ${e.message}`);
  }
}

/**
 * Deletes all columns after the specified number of columns to keep.
 * This prevents wasted cells when a sheet has many rows but few used columns.
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet The sheet to trim.
 * @param {number} columnsToKeep The number of columns to retain (1-indexed count).
 */
function AHA_TrimUnusedColumns3(sheet, columnsToKeep) {
  try {
    const maxCols = sheet.getMaxColumns();
    if (maxCols > columnsToKeep) {
      // Delete from column (columnsToKeep + 1) to the end
      sheet.deleteColumns(columnsToKeep + 1, maxCols - columnsToKeep);
      Logger.log(`Trimmed unused columns: kept ${columnsToKeep}, deleted ${maxCols - columnsToKeep} columns.`);
    }
  } catch (err) {
    Logger.log(`Warning: Could not trim columns - ${err.message}`);
  }
}


/**
 * Validates brands for the just-imported category and sends results to Slack.
 * Called at the end of the import process before the failed folder check.
 *
 * @param {string} category The category that was just imported.
 */
function AHA_ValidateCategoryBrands3(category) {
  const ADMIN_SHEET_ID = "11aYJSWTW7xxZcyfREdcvGoUhgPnl4xU8MQM6lE1se4M";

  try {
    Logger.log(`Starting post-import validation for ${category}`);

    // Extract marketplace code from category name (last 3 characters)
    const marketplaceCode = category.slice(-3).toUpperCase();
    if (!["SHO", "LAZ", "TIK", "TOK", "BSL"].includes(marketplaceCode)) {
      Logger.log(`Skipping validation for ${category} - no valid marketplace code`);
      return;
    }

    // Check if this is a BA Dash category (requires different validation)
    const isBADash = category.toLowerCase().startsWith("ba dash");
    if (isBADash) {
      AHA_ValidateBADashCategory3(category, marketplaceCode, ADMIN_SHEET_ID);
      return;
    }

    // Get expected brands from Admin Sheet's Brand Master
    const adminSS = SpreadsheetApp.openById(ADMIN_SHEET_ID);
    const brandMasterSheet = adminSS.getSheetByName("Brand Master");

    if (!brandMasterSheet) {
      Logger.log("Brand Master sheet not found in Admin Sheet");
      return;
    }

    // Get brands for this marketplace (Brand Master: Col A = Marketplace, Col B = Brand, data starts row 10)
    const lastRow = brandMasterSheet.getLastRow();
    if (lastRow < 10) {
      Logger.log("No brand data in Brand Master");
      return;
    }

    const brandData = brandMasterSheet.getRange(10, 1, lastRow - 9, 2).getValues();
    const expectedBrands = [];

    for (const row of brandData) {
      const rowMarketplace = (row[0] || "").toString().trim().toUpperCase();
      const brandCode = (row[1] || "").toString().trim().toUpperCase();

      if (rowMarketplace === marketplaceCode && brandCode && !brandCode.startsWith("(")) {
        expectedBrands.push(brandCode);
      }
    }

    if (expectedBrands.length === 0) {
      Logger.log(`No brands found in Brand Master for ${marketplaceCode}`);
      return;
    }

    // Get imported brands from the category sheet (Column A)
    const centralId = PropertiesService.getScriptProperties().getProperty('CENTRAL_SPREADSHEET_ID');
    const centralSS = SpreadsheetApp.openById(centralId);
    const categorySheet = centralSS.getSheetByName(category);

    if (!categorySheet) {
      Logger.log(`Category sheet '${category}' not found`);
      return;
    }

    const catLastRow = categorySheet.getLastRow();
    if (catLastRow < 2) {
      Logger.log("No data in category sheet");
      return;
    }

    const importedData = categorySheet.getRange(2, 1, catLastRow - 1, 1).getValues();
    const importedBrands = new Set();

    for (const row of importedData) {
      const brand = (row[0] || "").toString().trim().toUpperCase();
      if (brand) importedBrands.add(brand);
    }

    // Find missing brands
    const missingBrands = expectedBrands.filter(b => !importedBrands.has(b));

    // Build and send Slack message
    let message = `📊 *Brand Validation - ${category}*\n\n`;
    message += `Marketplace: *${marketplaceCode}*\n`;
    message += `Expected Brands: *${expectedBrands.length}*\n`;
    message += `Found Brands: *${importedBrands.size}*\n`;
    message += `Missing Brands: *${missingBrands.length}*\n`;

    if (missingBrands.length > 0) {
      const missingList = missingBrands.length <= 10
        ? missingBrands.join(", ")
        : missingBrands.slice(0, 10).join(", ") + ` ... and ${missingBrands.length - 10} more`;
      message += `\n⚠️ *Missing:* ${missingList}`;
    } else {
      message += `\n✅ All brands found!`;
    }

    AHA_SlackNotify3(message);
    Logger.log(`Validation complete for ${category}: ${missingBrands.length} missing`);

  } catch (e) {
    Logger.log(`Error in validation: ${e.message}`);
    AHA_SlackNotify3(`⚠️ Validation check failed for *${category}*: ${e.message}`);
  }
}


/**
 * Validates BA Dash categories with date-based validation (L7D).
 *
 * @param {string} category The BA Dash category.
 * @param {string} marketplaceCode The marketplace code.
 * @param {string} adminSheetId The Admin Sheet ID.
 */
function AHA_ValidateBADashCategory3(category, marketplaceCode, adminSheetId) {
  const BA_DASH_DATE_COLUMNS = {
    "BA Dash SHO": 18, // Column R
    "BA Dash LAZ": 2,  // Column B
    "BA Dash TIK": 2,  // Column B
    "BA Dash TOK": 2   // Column B
  };

  try {
    // Get expected brands from Admin Sheet
    const adminSS = SpreadsheetApp.openById(adminSheetId);
    const brandMasterSheet = adminSS.getSheetByName("Brand Master");

    if (!brandMasterSheet) {
      Logger.log("Brand Master sheet not found");
      return;
    }

    const lastRow = brandMasterSheet.getLastRow();
    if (lastRow < 10) return;

    const brandData = brandMasterSheet.getRange(10, 1, lastRow - 9, 2).getValues();
    const expectedBrands = [];

    for (const row of brandData) {
      const rowMarketplace = (row[0] || "").toString().trim().toUpperCase();
      const brandCode = (row[1] || "").toString().trim().toUpperCase();

      if (rowMarketplace === marketplaceCode && brandCode && !brandCode.startsWith("(")) {
        expectedBrands.push(brandCode);
      }
    }

    if (expectedBrands.length === 0) {
      Logger.log(`No brands for ${marketplaceCode}`);
      return;
    }

    // Get Central's Import sheet (clean data with dates)
    const centralId = PropertiesService.getScriptProperties().getProperty('CENTRAL_SPREADSHEET_ID');
    const centralSS = SpreadsheetApp.openById(centralId);
    const importSheet = centralSS.getSheetByName("Import");

    if (!importSheet) {
      Logger.log("Import sheet not found");
      return;
    }

    const dateColumn = BA_DASH_DATE_COLUMNS[category];
    if (!dateColumn) {
      Logger.log(`No date column mapping for ${category}`);
      return;
    }

    // Calculate L7D date range
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const endDate = new Date(today);
    endDate.setDate(endDate.getDate() - 1); // Yesterday
    const startDate = new Date(endDate);
    startDate.setDate(startDate.getDate() - 6); // 7 days back

    // Get data
    const importLastRow = importSheet.getLastRow();
    if (importLastRow < 2) {
      Logger.log("No data in Import sheet");
      return;
    }

    const maxCol = Math.max(1, dateColumn);
    const data = importSheet.getRange(2, 1, importLastRow - 1, maxCol).getValues();

    // Group brands by date and track duplicates
    const brandsByDate = {};
    const brandDateCounts = {};

    for (const row of data) {
      const brand = (row[0] || "").toString().trim().toUpperCase();
      const dateValue = row[dateColumn - 1];

      if (!brand || !dateValue) continue;

      let rowDate = dateValue instanceof Date ? new Date(dateValue) : new Date(dateValue);
      if (isNaN(rowDate.getTime())) continue;

      rowDate.setHours(0, 0, 0, 0);

      if (rowDate >= startDate && rowDate <= endDate) {
        const dateKey = Utilities.formatDate(rowDate, "Asia/Jakarta", "dd/MM/yyyy");
        if (!brandsByDate[dateKey]) brandsByDate[dateKey] = new Set();
        brandsByDate[dateKey].add(brand);

        const brandDateKey = `${dateKey}|${brand}`;
        brandDateCounts[brandDateKey] = (brandDateCounts[brandDateKey] || 0) + 1;
      }
    }

    // Find duplicates
    const duplicates = [];
    for (const [key, count] of Object.entries(brandDateCounts)) {
      if (count > 1) {
        const [dateKey, brand] = key.split("|");
        duplicates.push({ date: dateKey, brand: brand, count: count });
      }
    }

    // Build date results
    const dateResults = [];
    let datesWithMissing = 0;
    let currentDate = new Date(startDate);

    while (currentDate <= endDate) {
      const dateKey = Utilities.formatDate(currentDate, "Asia/Jakarta", "dd/MM/yyyy");
      const brandsOnDate = brandsByDate[dateKey] || new Set();
      const hasData = brandsOnDate.size > 0;
      const missingOnDate = hasData ? expectedBrands.filter(b => !brandsOnDate.has(b)) : [];

      if (hasData && missingOnDate.length > 0) datesWithMissing++;

      dateResults.push({
        date: dateKey,
        hasData: hasData,
        missingCount: hasData ? missingOnDate.length : 0,
        missingBrands: missingOnDate
      });

      currentDate.setDate(currentDate.getDate() + 1);
    }

    // Build Slack message
    let message = `📊 *Brand Validation - ${category}* (L7D)\n`;
    message += `Marketplace: *${marketplaceCode}*\n`;
    message += `Expected Brands: *${expectedBrands.length}*\n\n`;

    for (const dr of dateResults) {
      if (!dr.hasData) {
        message += `📅 ${dr.date} - ⚪ No data\n`;
      } else if (dr.missingCount === 0) {
        message += `📅 ${dr.date} - ✅ All brands found\n`;
      } else {
        const missingList = dr.missingBrands.slice(0, 5).join(", ");
        const extra = dr.missingBrands.length > 5 ? ` +${dr.missingBrands.length - 5} more` : "";
        message += `📅 ${dr.date} - ⚠️ Missing: ${missingList}${extra}\n`;
      }
    }

    message += `\n*Summary:* ${datesWithMissing} of ${dateResults.length} days with missing brands`;

    if (duplicates.length > 0) {
      message += `\n\n🔴 *Duplicates Found:* ${duplicates.length} brand+date combinations`;
      const dupsToShow = duplicates.slice(0, 5);
      for (const dup of dupsToShow) {
        message += `\n• ${dup.date} - ${dup.brand} (${dup.count}x)`;
      }
      if (duplicates.length > 5) {
        message += `\n_...and ${duplicates.length - 5} more duplicates_`;
      }
    } else {
      message += `\n\n✅ *No duplicates found*`;
    }

    AHA_SlackNotify3(message);
    Logger.log(`BA Dash validation complete for ${category}`);

  } catch (e) {
    Logger.log(`Error in BA Dash validation: ${e.message}`);
    AHA_SlackNotify3(`⚠️ BA Dash validation failed for *${category}*: ${e.message}`);
  }
}












