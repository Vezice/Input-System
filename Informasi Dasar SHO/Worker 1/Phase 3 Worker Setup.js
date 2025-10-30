////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Worker Copy.gs
//
// This script is fully dynamic and reads its configuration from script properties.
// To initialize, you must run the SETUP_SetWorkerConfiguration function once.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// ------------------------------------------------------------------------------------------
// --- CONFIGURATION AND SETUP --------------------------------------------------------------
// ------------------------------------------------------------------------------------------

/**
 * ONE-TIME SETUP FUNCTION FOR A SPECIFIC WORKER SPREADSHEET
 *
 * This function reads the worker's category and role from the 'Input' sheet,
 * then finds its corresponding Central Web App URL and Slack Webhook URL from
 * the 'Links List' sheet. It saves these values to the script's properties.
 *
 * You must run this manually from the script editor once for each worker
 * after setting up the 'Links List' sheet.
 */
function AHA_SetWorkerConfiguration3() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const properties = PropertiesService.getScriptProperties();

  // 1. Get the worker's configuration from the 'Input' sheet
  const inputSheet = ss.getSheetByName("Input");
  if (!inputSheet) {
    throw new Error("A 'Input' sheet is required to set the configuration.");
  }
  
  const workerCategory = inputSheet.getRange("C1").getValue();
  const workerNumber = inputSheet.getRange("D1").getValue();

  if (!workerCategory || !workerNumber) {
    throw new Error("Please set a category in cell C1 and a worker number in cell D1 of the 'Input' sheet.");
  }
  
  const workerRole = "Worker " + workerNumber;
  Logger.log(`Attempting to set configuration for Category: "${workerCategory}" and Role: "${workerRole}"`);

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
  const centralUrlCol = headers.indexOf("Apps Script Deployment URL");
  const slackUrlCol = headers.indexOf("Slack Webhook URL");

  if (categoryCol === -1 || roleCol === -1 || centralUrlCol === -1 || slackUrlCol === -1) {
    throw new Error("Could not find required columns in 'Links List'. Ensure you have 'Category / Type', 'Role', 'Apps Script Deployment URL', and 'Slack Webhook URL' columns.");
  }

  let foundConfiguration = false;

  // 4. Loop through the list to find the matching category and role
  for (const row of data) {
    const rowCategory = row[categoryCol];
    const rowRole = row[roleCol];

    if (rowCategory === workerCategory && rowRole === "Central") {
      properties.setProperty('CENTRAL_WEB_APP_URL', row[centralUrlCol]);
    }

    if (rowCategory === workerCategory && rowRole === workerRole) {
      properties.setProperty('SLACK_WEBHOOK_URL', row[slackUrlCol]);
      properties.setProperty('WORKER_CATEGORY', workerCategory);
      properties.setProperty('WORKER_COUNT', workerRole);
      foundConfiguration = true;
      Logger.log(`✅ Configuration found and set successfully.`);
      break; // Exit the loop once the correct row is found
    }
  }

  // 5. Handle success or failure
  if (!foundConfiguration) {
    throw new Error(`Could not find a matching configuration for Category: "${workerCategory}" and Role: "${workerRole}" in the 'Links List'. Please check your 'Input' sheet and 'Links List'.`);
  }
}

// ------------------------------------------------------------------------------------------
// --- MAIN OPERATIONAL SCRIPT --------------------------------------------------------------
// ------------------------------------------------------------------------------------------


// === Web App entry point ===
function doPost(e) {
  const secretToken = 'CuanCengliCincai';
  const data = JSON.parse(e.postData.contents || '{}');

  if (data.token !== secretToken) {
    return ContentService.createTextOutput("Unauthorized")
      .setMimeType(ContentService.MimeType.TEXT);
  }

  if (data.command === 'StartWorker') {
    // Schedule the worker to run shortly (time-driven trigger)
    // NOTE: Smallest practical delay is ~1 minute.
    ScriptApp.newTrigger('AHA_StartWorking3')
      .timeBased()
      .after(60 * 1000) // ~1 minute
      .create();

    return ContentService.createTextOutput("Worker scheduled")
      .setMimeType(ContentService.MimeType.TEXT);
  }

  return ContentService.createTextOutput("Invalid command")
    .setMimeType(ContentService.MimeType.TEXT);
}

function AHA_StartWorking3() {
  // --- NEW: MASTER RESET ---
  // Run this first to ensure a clean slate and prevent the "too many triggers" error.
  // This deletes all "zombie" triggers left from any previous crash.
  AHA_DeleteAllTimeBasedTriggers();
  // --- END NEW ---

  AHA_SetWorkerConfiguration3();
  const properties = PropertiesService.getScriptProperties();
  const category = properties.getProperty("WORKER_CATEGORY");
  const workerRole = properties.getProperty("WORKER_COUNT");

  if (!category || !workerRole) {
    throw new Error("Worker category or role not found in script properties. Please run AHA_SetWorkerConfiguration3 first.");
  }

  properties.setProperty('SYSTEM_STATUS', 'VALIDATING');
  
  properties.setProperty("LAST_VALIDATION_HEARTBEAT", new Date().getTime());
  
  AHA_SlackNotify3("✅ Worker activated. Status: VALIDATING.");
  
  // This line (109) will now succeed because the slots are free.
  ScriptApp.newTrigger("AHA_SystemWatchdog")
    .timeBased()
    .everyMinutes(15)
    .create();
  Logger.log("System Watchdog trigger created.");

  AHA_StartValidation2();
}

function AHA_CheckURL(){
  const properties = PropertiesService.getScriptProperties();
  AHA_SlackNotify3(properties.getProperty("CENTRAL_WEB_APP_URL"));
}

// This function sends a notification to the central sheet.
function AHA_NotifyCentral3() {
  const start = new Date();
  try {
    const properties = PropertiesService.getScriptProperties();
    const category = properties.getProperty("WORKER_CATEGORY");
    const worker = properties.getProperty("WORKER_COUNT");
    const url = properties.getProperty("CENTRAL_WEB_APP_URL");

    if (!category || !worker || !url) {
      const errorMsg = "FATAL ERROR: Missing configuration properties (Category, Role, or Central URL). Please run AHA_SetWorkerConfiguration3.";
      Logger.log(errorMsg);
      // We can't use our Slack notifier if its own config might be missing, so this is a hard throw.
      throw new Error(errorMsg);
    }

    const payload = JSON.stringify({
      category: category,
      worker: worker
    });

    const options = {
      method: 'post',
      contentType: 'application/json',
      payload: payload,
      muteHttpExceptions: true // Keep true to inspect response codes
    };

    // Use the retry helper
    AHA_ExecuteWithRetry(() => {
      const response = UrlFetchApp.fetch(url, options);
      const responseText = response.getContentText();
      const responseCode = response.getResponseCode();

      if (responseCode === 200 && responseText === 'OK') {
        Logger.log(`✅ Central sheet notified successfully. Category: ${category}, Worker: ${worker}`);
        AHA_SlackNotify3(`✅ Worker *${worker}* for *${category}* reported 'Done' to central sheet.`);
        return; // Success, exit retry loop
      } else if (responseCode === 200 && responseText === 'SERVER_BUSY') {
        throw new Error('Server is busy, will retry.'); // Throw an error to trigger the retry
      } else {
        // For other errors, we also throw to retry, but might fail eventually.
        throw new Error(`Central sheet returned error: Code ${responseCode}, Response: ${responseText}`);
      }
    }, 'Notify Central Sheet', 5, 5000); // Retry 5 times, starting with a 5-second delay

  } catch (err) {
    Logger.log(`❌ Failed to notify central sheet after all retries: ${err.message}`);
    AHA_SlackNotify3(`❌ Worker *${properties.getProperty("WORKER_COUNT")}* for *${properties.getProperty("WORKER_CATEGORY")}*: Failed to notify central sheet after all retries. <@U08TUF8LW2H>`);
  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}

/**
 * --- GENERIC RETRY HELPER ---
 * Executes a function and retries it on failure with an exponential backoff delay.
 * @param {Function} func The function to execute.
 * @param {string} operationName A descriptive name for the operation for logging.
 * @param {number} maxRetries The maximum number of times to retry.
 * @param {number} initialDelayMs The starting delay in milliseconds.
 * @returns The return value of the successful function execution.
 * @throws {Error} Throws the last error if all retries fail.
 */
function AHA_ExecuteWithRetry(func, operationName = 'Unnamed Operation', maxRetries = 3, initialDelayMs = 2000) {
  let attempt = 0;
  let delay = initialDelayMs;
  while (attempt < maxRetries) {
    try {
      return func(); // Attempt to execute the function
    } catch (err) {
      attempt++;
      Logger.log(`⚠️ Attempt ${attempt}/${maxRetries} failed for '${operationName}': ${err.message}`);
      if (attempt >= maxRetries) {
        AHA_SlackNotify3(`❌ *FATAL ERROR*: Operation '${operationName}' failed after ${maxRetries} attempts. Last error: ${err.message}`);
        throw new Error(`Operation '${operationName}' failed after ${maxRetries} attempts. Last error: ${err.message}`);
      }
      // Use exponential backoff with jitter
      const jitter = Math.random() * 1000;
      Utilities.sleep(delay + jitter);
      delay *= 2; // Double the delay for the next attempt
    }
  }
}