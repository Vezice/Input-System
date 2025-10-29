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
  AHA_SlackNotify3(`Configuration set. Central: ${foundCentral}, Workers: ${workersFound}`);
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
      AHA_SlackNotify3(`❌ *Fatal Error in doPost*: ${error.message} <@U08TUF8LW2H>`);
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
  AHA_SlackNotify3(`✅ *${worker}* for *${category}* status updated to 'Done'.`);

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
    AHA_SlackNotify3(`⏳ *Workers Pending*: Not all workers for *${category}* are done yet.`);
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
  const ackText = `👀 Processing your request, ${user}... *Adding a 5-minute delay* to allow Google Drive to sync. File processing will begin shortly.`;
  const slackPayload = {
    method: "post",
    contentType: "application/json",
    payload: JSON.stringify({ text: ackText })
  };
  UrlFetchApp.fetch(e.parameter.response_url, slackPayload);
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
    AHA_SlackNotify3(`❌ *Fatal Error*: The 5-minute trigger failed to start the workers: ${e.message} <@U08TUF8LW2H>`);
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
    AHA_SlackNotify3(`❌ *Merge Process Error*: 'System Dashboard' sheet not found. <@U08TUF8LW2H>`);
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
    AHA_SlackNotify3(`✨ *Merge Process*: Created and formatted new temporary sheet: ${tempSheetName}.`);
  } else {
    tempSheet = targetSS.getSheetByName(tempSheetName);
    if (!tempSheet) {
      Logger.log(`ERROR: Temp sheet '${tempSheetName}' not found. Stopping.`);
      AHA_SlackNotify3(`❌ *Merge Process Error*: Failed to create temporary sheet: ${tempSheetName}. <@U08TUF8LW2H>`);
      AHA_DeleteTriggerByName3('AHA_ProcessMergeBatch3');
      return;
    }
  }

  const worker = workersToProcess[workerIndex];
  const sourceId = properties.getProperty(worker); // Dynamic lookup, e.g., getProperty('Worker 1')
  
  if (!sourceId) {
    Logger.log(`ERROR: Source ID for worker '${worker}' not found in properties. Stopping merge.`);
    AHA_SlackNotify3(`❌ *Merge Process Error*: Source ID for worker '${worker}' not found in properties. Stopping merge. <@U08TUF8LW2H>`);
    AHA_DeleteTriggerByName3('AHA_ProcessMergeBatch3');
    return;
  }
  const sourceSS = SpreadsheetApp.openById(sourceId);
  const sourceSheet = sourceSS.getSheetByName(category);
  
  if (!sourceSheet) {
    Logger.log(`ERROR: Source sheet '${category}' not found in worker '${worker}'. Stopping merge.`);
    AHA_SlackNotify3(`❌ *Merge Process Error*: Temporary sheet '${tempSheetName}' not found. Stopping. <@U08TUF8LW2H>`);
    AHA_DeleteTriggerByName3('AHA_ProcessMergeBatch3');
    return;
  }
  
  const dataToCopy = sourceSheet.getDataRange().getDisplayValues();

  if (dataToCopy && dataToCopy.length > 0) {
    if (workerIndex === 0 && tempSheet) { 
      const headers = [dataToCopy[0]];
      tempSheet.getRange(1, 1, 1, headers[0].length).setValues(headers).setFontWeight("bold").setBackground("yellow");
    }
    const rowsToCopy = dataToCopy.slice(1);
    if (rowsToCopy.length > 0 && tempSheet) { 
      tempSheet.getRange(tempSheet.getLastRow() + 1, 1, rowsToCopy.length, rowsToCopy[0].length).setValues(rowsToCopy);
      AHA_SlackNotify3(`➡️ *Merge Process*: Copied ${rowsToCopy.length} rows from *${worker}* for *${category}*.`);
      Logger.log(`Copied ${rowsToCopy.length} rows from ${worker} for ${category}.`);
    }
  } else {
    Logger.log(`${worker} for ${category} had no data to copy.`);
    AHA_SlackNotify3(`⚠️ *Merge Process*: *${worker}* for *${category}* had no data to copy.`);
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
    AHA_SlackNotify3(`❌ *BA Dash Merge Error*: Central Spreadsheet ID not found. Stopping BA Dash merge. <@U08TUF8LW2H>`);
    AHA_DeleteTriggerByName3('AHA_ProcessBADashMergeBatch3');
    return;
  }
  if (!category || !workerIndexStr) {
    Logger.log("BA Dash Merge triggered without properties. Stopping.");
    AHA_SlackNotify3(`❌ *BA Dash Merge Error*: BA Dash Merge triggered without properties. Stopping. <@U08TUF8LW2H>`);
    AHA_DeleteTriggerByName3('AHA_ProcessBADashMergeBatch3');
    return;
  }

  const workerIndex = parseInt(workerIndexStr, 10);
  const targetSS = SpreadsheetApp.openById(centralId);
  const controlSheet = targetSS.getSheetByName('System Dashboard');
  
  if (!controlSheet) {
    Logger.log(`ERROR: 'System Dashboard' sheet not found. Stopping BA Dash merge.`);
    AHA_SlackNotify3(`❌ *BA Dash Merge Error*: 'System Dashboard' sheet not found. <@U08TUF8LW2H>`);
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
      const originalData = originalSheet.getDataRange().getValues();
      if (originalData.length > 0) {
        tempSheet.getRange(1, 1, originalData.length, originalData[0].length).setValues(originalData);
        AHA_SlackNotify3(`📋 *BA Dash Merge*: Copied ${originalData.length} existing rows from *${category}*.`);
        Logger.log(`Copied ${originalData.length} existing rows from '${category}'.`);
      }
    }
  } else {
    tempSheet = targetSS.getSheetByName(tempSheetName);
    if (!tempSheet) {
      Logger.log(`ERROR: Temp sheet '${tempSheetName}' not found for BA Dash merge. Stopping.`);
      AHA_SlackNotify3(`❌ *BA Dash Merge Error*: Temp sheet '${tempSheetName}' not found. <@U08TUF8LW2H>`);
      AHA_DeleteTriggerByName3('AHA_ProcessBADashMergeBatch3');
      return;
    }
  }

  const worker = workersToProcess[workerIndex];
  const sourceId = properties.getProperty(worker); // Dynamic lookup
  
  if (!sourceId) {
    Logger.log(`ERROR: Source ID for worker '${worker}' not found. Stopping BA Dash merge.`);
    AHA_SlackNotify3(`❌ *BA Dash Merge Error*: Source ID for '${worker}' not found. <@U08TUF8LW2H>`);
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
        tempSheet.getRange(tempSheet.getLastRow() + 1, 1, rowsToCopy.length, rowsToCopy[0].length).setValues(rowsToCopy);
        Logger.log(`Appended ${rowsToCopy.length} rows from ${worker} for ${category}.`);
        AHA_SlackNotify3(`➡️ *BA Dash Merge*: Appended ${rowsToCopy.length} rows from *${worker}* for *${category}*.`);
      }
    } else {
      Logger.log(`${worker} for ${category} had no data rows to append.`);
      AHA_SlackNotify3(`⚠️ *BA Dash Merge*: ${worker} for ${category} had no data rows to append. Skipping.`);
    }
  } else {
    Logger.log(`Source sheet '${category}' not found in worker '${worker}'. Skipping.`);
    AHA_SlackNotify3(`⚠️ *BA Dash Merge*: Source sheet '${category}' not found in *${worker}*. Skipping.`);
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
    AHA_SlackNotify3(`🗑️ *Merge Process*: Deleted old category sheet: *${category}*.`);
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
  AHA_SlackNotify3(`📊 *Dashboard Update*: Status for category *${category}* set to 'Imported'.`);
  
  controlSheet.getRange("A1").setValue("SCRIPT OFFLINE");
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
 * Checks the central 'Failed' folder, reads the 'FAILURE_LOG' Google Doc,
 * and sends a smart, cross-referenced report to Slack.
 */
function AHA_CheckFailedImports3() {
  // --- CONFIGURATION ---
  // This must be the SAME ID you put in the Worker's CONFIG.
  const FAILURE_LOG_DOC_ID = "1oiJtDXBLIFq_LEa9yypEw7R8ZBfon3mQ6L4i8EqhWDQ";
  const MENTION_USER_ON_ERROR = "<@U08TUF8LW2H>"; // <-- FIX: Hardcode the user ID here
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
      filesInFolder.set(file.getName(), file.getDateCreated());
    }

    if (filesInFolder.size === 0) {
      AHA_SlackNotify3(`✅ The central 'Failed' folder is empty. All good!`);
      return;
    }

    // --- NEW LOGIC: Read and Parse the Google Doc ---
    const failureLog = new Map();
    try {
      const doc = DocumentApp.openById(FAILURE_LOG_DOC_ID);
      const logText = doc.getBody().getText();
      const logLines = logText.split('\n').filter(Boolean); // Split by line, remove blanks
      
      for (const line of logLines) {
        const parts = line.split(' | ');
        if (parts.length < 4) continue; // Skip malformed lines

        const dateStr = parts[0].replace('[', '').replace(']', '');
        const fileName = parts[1].trim();
        const reason = parts[2].trim();
        const workerInfo = parts[3].trim();
        const logDate = new Date(dateStr);

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
      // --- FIX: Use the new hardcoded variable ---
      AHA_SlackNotify3(`❌ *Error*: Could not read the Failure Log Doc: ${docErr.message} ${MENTION_USER_ON_ERROR}`);
      // Continue without log data if it fails
    }

    // --- Build the Smart Slack Message ---
    let message = `🚨 *Failed Imports Report!* ${filesInFolder.size} file(s) are in the 'Failed' folder:\n\n`;
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
    // --- FIX: Use the new hardcoded variable ---
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
















