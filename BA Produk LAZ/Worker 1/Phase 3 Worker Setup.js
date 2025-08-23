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
  const secretToken = 'CuanCengliCincai'; // match this in central sheet
  const data = JSON.parse(e.postData.contents);

  if (data.token !== secretToken) {
    return ContentService.createTextOutput("Unauthorized").setMimeType(ContentService.MimeType.TEXT);
  }

  if (data.command === 'StartWorker') {
    try {
      AHA_StartWorking3();
      return ContentService.createTextOutput("Worker started successfully").setMimeType(ContentService.MimeType.TEXT);
    } catch (err) {
      return ContentService.createTextOutput("Error: " + err.message).setMimeType(ContentService.MimeType.TEXT);
    }
  }

  return ContentService.createTextOutput("Invalid command").setMimeType(ContentService.MimeType.TEXT);
}

function AHA_StartWorking3() {
  AHA_SetWorkerConfiguration3();
  const properties = PropertiesService.getScriptProperties();
  const category = properties.getProperty("WORKER_CATEGORY");
  const workerRole = properties.getProperty("WORKER_COUNT");

  if (!category || !workerRole) {
    throw new Error("Worker category or role not found in script properties. Please run AHA_SetWorkerConfiguration3 first.");
  }
  AHA_StartValidation2();
}

function AHA_CheckURL(){
  const properties = PropertiesService.getScriptProperties();
  AHA_SlackNotify3(properties.getProperty("CENTRAL_WEB_APP_URL"));
}

// This function sends a notification to the central sheet.
function AHA_NotifyCentral3() {
  const start = new Date();
  const properties = PropertiesService.getScriptProperties();
  const category = properties.getProperty("WORKER_CATEGORY");
  const worker = properties.getProperty("WORKER_COUNT");
  const url = properties.getProperty("CENTRAL_WEB_APP_URL");

  if (!category || !worker || !url) {
    const errorMsg = "FATAL ERROR: Missing configuration properties (Category, Role, or Central URL). Please run AHA_SetWorkerConfiguration3.";
    Logger.log(errorMsg);
    AHA_SlackNotify3(`❌ *FATAL ERROR*: ${errorMsg}`);
    throw new Error(errorMsg);
  }
  
  const MAX_RETRIES = 5;
  const RETRY_DELAY_MS = 5000; // 5 seconds

  const payload = {
    category: category,
    worker: worker
  };

  const options = {
    method: 'post',
    // contentType: 'application/x-www-form-urlencoded',
    // payload: `category=${encodeURIComponent(category)}&worker=${encodeURIComponent(worker)}`,
    contentType: 'application/json',
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const response = UrlFetchApp.fetch(url, options);
      const responseText = response.getContentText();
      const responseCode = response.getResponseCode();

      if (responseCode === 200 && responseText === 'OK') {
        Logger.log(`✅ Central sheet notified successfully. Category: ${category}, Worker: ${worker}`);
        AHA_SlackNotify3(`✅ Worker *${worker}* for *${category}* reported 'Done' to central sheet.`);
        return; // Success, exit function
      } else if (responseCode === 200 && responseText === 'SERVER_BUSY') {
        Logger.log(`⚠️ Central sheet is busy. Retrying... (Attempt ${attempt}/${MAX_RETRIES})`);
        AHA_SlackNotify3(`⚠️ Worker *${worker}* for *${category}*: Central sheet busy. Retrying...`);
        Utilities.sleep(RETRY_DELAY_MS); // Wait before retrying
      } else {
        Logger.log(`❌ Central sheet returned error: Code ${responseCode}, Response: ${responseText}`);
        AHA_SlackNotify3(`❌ Worker *${worker}* for *${category}*: Central sheet error: ${responseText} (Code: ${responseCode}) <@U08TUF8LW2H>`);
        if (attempt < MAX_RETRIES) {
          Utilities.sleep(RETRY_DELAY_MS);
        } else {
          throw new Error(`Failed to notify central sheet after ${MAX_RETRIES} attempts. Last error: ${responseText}`);
        }
      }
    } catch (err) {
      Logger.log(`❌ Network/Fetch error when notifying central sheet (Attempt ${attempt}/${MAX_RETRIES}): ${err.message}`);
      AHA_SlackNotify3(`❌ Worker *${worker}* for *${category}*: Network error notifying central sheet: ${err.message} <@U08TUF8LW2H>`);
      if (attempt < MAX_RETRIES) {
        Utilities.sleep(RETRY_DELAY_MS);
      } else {
        throw new Error(`Failed to notify central sheet after ${MAX_RETRIES} attempts due to network error: ${err.message}`);
      }
    }
  }
  Logger.log(`❌ Failed to notify central sheet after ${MAX_RETRIES} attempts.`);
  AHA_SlackNotify3(`❌ Worker *${worker}* for *${category}*: Failed to notify central sheet after ${MAX_RETRIES} attempts. <@U08TUF8LW2H>`);

  AHA_LogRuntime3(new Date() - start);
}












