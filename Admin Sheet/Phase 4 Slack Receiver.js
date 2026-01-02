/**
 * --- STEP 1: Main Web App Function ---
 * Receives a Slack command, stores the command details, creates a temporary
 * trigger to process it, and responds to Slack immediately.
 *
 * @param {Object} e The event parameter from the web app request.
 */
function doPost(e) {
  try {
    // Generate a unique ID for this job to safely handle multiple simultaneous requests.
    const jobId = `job_${new Date().getTime()}_${Math.floor(Math.random() * 1000)}`;
    
    // Store the entire payload from Slack. This contains all the necessary info (text, user_name, etc.).
    PropertiesService.getScriptProperties().setProperty(jobId, e.postData.contents);

    // Create a trigger that will fire ONCE in 5 seconds.
    // This makes the process asynchronous and prevents timeouts.
    const trigger = ScriptApp.newTrigger('processAndForwardCommand')
      .timeBased()
      .after(5 * 1000) // 5 seconds from now
      .create();

    // Store a mapping between the trigger's unique ID and our job ID.
    // This is how the triggered function will know which data to process.
    PropertiesService.getScriptProperties().setProperty(trigger.getUniqueId(), jobId);
    
    Logger.log(`Created one-time trigger ${trigger.getUniqueId()} for job ${jobId}`);

    // Immediately respond to Slack. This response must be sent within 3 seconds.
    const slackResponse = {
      "response_type": "ephemeral", // "ephemeral" is a private response visible only to the user.
      "text": `✅ Command *${e.parameter.text}* will be processed in 1 minute.`
    };

    return ContentService.createTextOutput(JSON.stringify(slackResponse)).setMimeType(ContentService.MimeType.JSON);

  } catch (error) {
    Logger.log(`FATAL Error in Admin doPost: ${error.toString()}`);
    // This response is a last resort if the initial setup fails.
    // return ContentService.createTextOutput(`A fatal error occurred before your command could be processed: ${error.message}`).setMimeType(ContentService.MimeType.TEXT);
  }
}

/**
 * --- STEP 2: Triggered Function ---
 * This function is executed ONCE by the temporary trigger created in doPost.
 * It checks for recent duplicates, forwards the command, and cleans up after itself.
 *
 * @param {Object} e The event object from the time-based trigger.
 */
function processAndForwardCommand(e) {
  // --- CONFIGURATION ---
  const linksSheetName = "List";
  const categoryColumn = 1; // Column A
  const urlColumn = 7;      // Column G
  const ROOT_DRIVE_FOLDER_ID = '0AJyZWtXd1795Uk9PVA';
  // --------------------

  const triggerId = e.triggerUid;
  const scriptProperties = PropertiesService.getScriptProperties();
  const jobId = scriptProperties.getProperty(triggerId);

  if (!jobId) {
    Logger.log(`Orphaned trigger run (ID: ${triggerId}). No job ID found. Cleaning up.`);
    cleanup(triggerId, null);
    return;
  }

  const payloadContents = scriptProperties.getProperty(jobId);
  if (!payloadContents) {
    Logger.log(`Could not find payload for job ID: ${jobId}. Cleaning up trigger ${triggerId}.`);
    cleanup(triggerId, jobId);
    return;
  }
  
  const payloadParams = payloadContents.split('&').reduce((acc, part) => {
    const item = part.split('=');
    acc[decodeURIComponent(item[0])] = decodeURIComponent(item[1].replace(/\+/g, ' '));
    return acc;
  }, {});

  try {
    const commandText = (payloadParams.text || "").trim();
    const commandParts = commandText.toLowerCase().split(/\s+/);
    const mainCommand = commandParts[0];
    const subCommand = commandParts.slice(1).join(' ');

    // --- NEW: DUPLICATE COMMAND CHECK ---
    const cache = CacheService.getScriptCache();
    const lastRunKey = `last_run_${commandText.toLowerCase().replace(/\s+/g, '_')}`;
    const lastRunTimestamp = cache.get(lastRunKey);
    const currentTime = new Date().getTime();
    const FIVE_MINUTES_IN_MS = 5 * 60 * 1000;

    if (lastRunTimestamp && (currentTime - parseInt(lastRunTimestamp)) < FIVE_MINUTES_IN_MS) {
      Logger.log(`Ignoring duplicate command "${commandText}" received within 5 minutes.`);
      sendSlackResponse(payloadParams.response_url, `⚠️ Command *${commandText}* was ignored because it was already processed in the last 5 minutes.`);
      return; // Stop execution
    }
    
    // If the command is valid, store the current time as the new "last run" timestamp.
    // The cache will automatically expire this entry after 5 minutes.
    cache.put(lastRunKey, currentTime.toString(), 300);
    // --- END OF NEW LOGIC ---

    // --- COMMAND ROUTER ---
    switch (mainCommand) {
      case 'test':
        const userName = payloadParams.user_name || 'user';
        sendSlackResponse(payloadParams.response_url, `Hello, ${userName}! 👋 Your test was successful.`);
        break;

      case 'check':
        if (subCommand === 'move') {
          handleFolderCheck(payloadParams.response_url, ROOT_DRIVE_FOLDER_ID, 'Move');
        } else if (subCommand === 'failed') {
          handleFolderCheck(payloadParams.response_url, ROOT_DRIVE_FOLDER_ID, 'Failed');
        } else if (subCommand === 'other') {
          handleOtherFoldersCheck(payloadParams.response_url, ROOT_DRIVE_FOLDER_ID);
        } else {
          sendSlackResponse(payloadParams.response_url, "Unknown 'check' command. Please use 'check move', 'check failed', or 'check other'.");
        }
        break;

      case 'validate':
        if (subCommand) {
          // Validate specific category
          handleBrandValidation(payloadParams.response_url, subCommand);
        } else {
          sendSlackResponse(payloadParams.response_url, "Please specify a category. Example: `validate BA Produk SHO`");
        }
        break;

      case 'dashboard':
        // Send link to the dashboard
        const dashboardUrl = `https://docs.google.com/spreadsheets/d/${SpreadsheetApp.getActiveSpreadsheet().getId()}/edit#gid=0`;
        sendSlackResponse(payloadParams.response_url, `📊 *iBot Dashboard*\n\n${dashboardUrl}`);
        break;

      case 'help':
        handleHelpCommand(payloadParams.response_url);
        break;

      case 'status':
        handleStatusCommand(payloadParams.response_url);
        break;

      case 'history':
        handleHistoryCommand(payloadParams.response_url);
        break;

      case 'validateall':
        handleValidateAllCommand(payloadParams.response_url);
        break;

      case 'pingall':
        handlePingAllCommand(payloadParams.response_url);
        break;

      default:
        // If no other command matches, assume it's a category to be forwarded.
        const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(linksSheetName);
        if (!sheet) throw new Error(`Sheet "${linksSheetName}" not found.`);
        
        const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, urlColumn).getValues();
        let targetUrl = null;

        for (let i = 0; i < data.length; i++) {
          if (data[i][categoryColumn - 1].toString().trim().toLowerCase() === commandText.toLowerCase()) {
            targetUrl = data[i][urlColumn - 1];
            break;
          }
        }

        if (!targetUrl) {
          throw new Error(`No URL found for category or command: "${commandText}"`);
        }

        const options = {
          'method': 'post',
          'contentType': 'application/x-www-form-urlencoded',
          'payload': payloadContents,
          'muteHttpExceptions': true
        };

        Logger.log(`Forwarding job ${jobId} for category "${commandText}" to URL: ${targetUrl}`);
        const response = UrlFetchApp.fetch(targetUrl, options);
        Logger.log(`Response from Central Sheet for job ${jobId}: ${response.getResponseCode()} - ${response.getContentText()}`);
        break;
    }
  } catch (error) {
    Logger.log(`Error during triggered forwarding for job ${jobId} (Trigger: ${triggerId}): ${error.toString()}`);
    sendSlackResponse(payloadParams.response_url, `❌ An error occurred: ${error.message}`);
  } finally {
    cleanup(triggerId, jobId);
  }
}

/**
 * --- Helper Function for Cleanup ---
 * Deletes the trigger and script properties associated with a job.
 */
function cleanup(triggerId, jobId) {
  const scriptProperties = PropertiesService.getScriptProperties();
  
  // Delete the trigger that just ran.
  ScriptApp.getProjectTriggers().forEach(trigger => {
    if (trigger.getUniqueId() === triggerId) {
      ScriptApp.deleteTrigger(trigger);
      Logger.log(`Deleted one-time trigger: ${triggerId}`);
    }
  });

  // Delete the script properties.
  if (jobId) {
    scriptProperties.deleteProperty(jobId);
    Logger.log(`Deleted property for job: ${jobId}`);
  }
  scriptProperties.deleteProperty(triggerId); // Delete the trigger-to-job mapping
  Logger.log(`Deleted trigger map property for: ${triggerId}`);
}


/**
 * Sends a response back to a Slack channel using the response_url.
 * @param {string} responseUrl The temporary URL provided by Slack for responding.
 * @param {string} text The message text to send.
 */
function sendSlackResponse(responseUrl, text) {
  // Always log
  Logger.log(`Slack Response: ${text}`);

  // Also write to the Admin Log sheet for visibility
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let logSheet = ss.getSheetByName("Command Log");
    if (!logSheet) {
      logSheet = ss.insertSheet("Command Log");
      logSheet.getRange("A1:C1").setValues([["Timestamp", "Type", "Message"]]).setFontWeight("bold");
    }
    logSheet.insertRowAfter(1);
    logSheet.getRange(2, 1, 1, 3).setValues([[new Date(), "RESPONSE", text.substring(0, 500)]]);
  } catch (e) {
    Logger.log("Could not write to Command Log: " + e.message);
  }

  // Send to Slack
  if (!responseUrl) {
    Logger.log("Cannot send Slack response: response_url is missing.");
    return;
  }

  let finalText = text;
  if (text.length > 2900) {
    finalText = text.substring(0, 2900) + "\n...(truncated)";
  }

  const payload = {
    response_type: "in_channel",
    text: "System Admin: " + finalText
  };
  const options = {
    method: "post",
    contentType: "application/json",
    payload: JSON.stringify(payload)
  };

  Logger.log(`Sending to Slack (${finalText.length} chars)`);
  UrlFetchApp.fetch(responseUrl, options);
  Logger.log(`Slack response sent`);
}

/**
 * Recursively finds all files within a folder and its subfolders, building a full path.
 * @param {GoogleAppsScript.Drive.Folder} folder The folder to start searching from.
 * @param {string} currentPath The path built so far.
 * @param {string[]} filePaths An array to accumulate the full paths of found files.
 */
function recursivelyFindFiles(folder, currentPath, filePaths) {
  // Find files in the current folder
  const files = folder.getFiles();
  while (files.hasNext()) {
    const file = files.next();
    filePaths.push(`${currentPath}/${file.getName()}`);
  }

  // Find subfolders and call this function for each one
  const subFolders = folder.getFolders();
  while (subFolders.hasNext()) {
    const subFolder = subFolders.next();
    recursivelyFindFiles(subFolder, `${currentPath}/${subFolder.getName()}`, filePaths);
  }
}

/**
 * Initiates a recursive search of a specific folder (e.g., "Move") and reports the full paths of all files found.
 * @param {string} responseUrl The Slack response URL.
 * @param {string} rootFolderId The ID of the main parent Drive folder.
 * @param {string} folderNameToFind The name of the top-level folder to check.
 */
function handleFolderCheck(responseUrl, rootFolderId, folderNameToFind) {
  try {
    const rootFolder = DriveApp.getFolderById(rootFolderId);
    const parentFolders = rootFolder.getFoldersByName(folderNameToFind);

    if (!parentFolders.hasNext()) {
      sendSlackResponse(responseUrl, `⚠️ Folder "${folderNameToFind}" could not be found.`);
      return;
    }

    const parentFolder = parentFolders.next();
    const allFilePaths = [];
    
    // Start the recursive search
    recursivelyFindFiles(parentFolder, parentFolder.getName(), allFilePaths);

    if (allFilePaths.length > 0) {
      const message = `📂 Full file paths found in *${folderNameToFind}*:\n• ${allFilePaths.join('\n• ')}`;
      sendSlackResponse(responseUrl, message);
    } else {
      sendSlackResponse(responseUrl, `✅ The *${folderNameToFind}* folder and all its subfolders are empty.`);
    }
  } catch (e) {
    sendSlackResponse(responseUrl, `❌ Error checking folder "${folderNameToFind}": ${e.message}`);
  }
}

/**
 * Initiates a recursive search of all folders EXCEPT excluded ones and reports the full paths of all files found.
 * @param {string} responseUrl The Slack response URL.
 * @param {string} rootFolderId The ID of the main parent Drive folder.
 */
function handleOtherFoldersCheck(responseUrl, rootFolderId) {
  const EXCLUDED_FOLDERS = ['Archive', 'Failed', 'Move'];
  try {
    const rootFolder = DriveApp.getFolderById(rootFolderId);
    const allFolders = rootFolder.getFolders();
    const allFilePaths = [];

    while (allFolders.hasNext()) {
      const folder = allFolders.next();
      const folderName = folder.getName();

      if (!EXCLUDED_FOLDERS.includes(folderName)) {
        // Start a recursive search for each top-level folder
        recursivelyFindFiles(folder, folderName, allFilePaths);
      }
    }

    if (allFilePaths.length > 0) {
      const message = `🔎 Full file paths found in other folders:\n• ${allFilePaths.join('\n• ')}`;
      sendSlackResponse(responseUrl, message);
    } else {
      sendSlackResponse(responseUrl, "✅ No content found in any other folders.");
    }
  } catch (e) {
    sendSlackResponse(responseUrl, `❌ Error checking other folders: ${e.message}`);
  }
}


/**
 * Handles brand validation for a specific category via Slack command.
 * Supports optional date range for BA Dash categories.
 *
 * Command formats:
 * - /ibot validate BA Produk SHO         → Standard validation
 * - /ibot validate BA Dash SHO           → BA Dash with L7D (default)
 * - /ibot validate BA Dash SHO L7D       → BA Dash with explicit range
 * - /ibot validate BA Dash SHO yesterday → BA Dash yesterday only
 * - /ibot validate BA Dash SHO L30D      → BA Dash last 30 days
 *
 * @param {string} responseUrl The Slack response URL.
 * @param {string} commandInput The full command input after "validate".
 */
function handleBrandValidation(responseUrl, commandInput) {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const listSheet = ss.getSheetByName("List");

    if (!listSheet) {
      sendSlackResponse(responseUrl, "❌ List sheet not found in Admin Sheet.");
      return;
    }

    // Parse command input - check for date range at the end
    const validDateRanges = ["yesterday", "l7d", "l30d"];
    const inputParts = commandInput.trim().split(/\s+/);
    let dateRange = null;
    let categoryInput = commandInput.trim();

    // Check if last part is a date range
    if (inputParts.length > 1) {
      const lastPart = inputParts[inputParts.length - 1].toLowerCase();
      if (validDateRanges.includes(lastPart)) {
        dateRange = lastPart.toUpperCase();
        if (dateRange === "YESTERDAY") dateRange = "yesterday";
        categoryInput = inputParts.slice(0, -1).join(" ");
      }
    }

    // List sheet columns:
    // A: Category/Type, B: Role, C: Link, D: Apps Script, E: Upload Folder,
    // F: Spreadsheet ID, G: Apps Script Deployment URL, H: Slack Webhook URL, I: Deployment ID
    const listData = listSheet.getRange(2, 1, listSheet.getLastRow() - 1, 6).getValues();
    let centralSheetId = null;
    let matchedCategory = null;

    for (const row of listData) {
      const category = (row[0] || "").toString().trim();
      if (category.toLowerCase() === categoryInput.toLowerCase()) {
        centralSheetId = row[5]; // Column F (index 5) contains Spreadsheet ID
        matchedCategory = category;
        break;
      }
    }

    if (!centralSheetId) {
      sendSlackResponse(responseUrl, `❌ Category "${categoryInput}" not found in List sheet. Please check the category name.`);
      return;
    }

    // Extract marketplace code from category name (last 3 characters: SHO, LAZ, TIK, TOK)
    const marketplaceCode = matchedCategory.slice(-3).toUpperCase();
    if (!["SHO", "LAZ", "TIK", "TOK"].includes(marketplaceCode)) {
      sendSlackResponse(responseUrl, `⚠️ Could not determine marketplace code from "${matchedCategory}". Expected ending: SHO, LAZ, TIK, or TOK.`);
      return;
    }

    // Check if this is a BA Dash category (requires date-based validation)
    const isBADash = matchedCategory.toLowerCase().startsWith("ba dash");

    if (isBADash) {
      // BA Dash categories use date-based validation
      dateRange = dateRange || "L7D"; // Default to L7D
      sendSlackResponse(responseUrl, `🔍 Starting BA Dash validation for *${matchedCategory}* (${dateRange})...`);

      const result = AHA_ValidateBADashBrands(matchedCategory, centralSheetId, marketplaceCode, dateRange);

      if (!result.success) {
        sendSlackResponse(responseUrl, `❌ Validation failed: ${result.error}`);
        return;
      }

      if (result.skipped) {
        sendSlackResponse(responseUrl, `⚠️ Validation skipped: ${result.reason}`);
        return;
      }

      // Build BA Dash response with per-date breakdown
      let message = `📊 *Brand Validation - ${matchedCategory}* (${dateRange})\n`;
      message += `Marketplace: *${marketplaceCode}*\n`;
      message += `Expected Brands: *${result.expectedCount}*\n\n`;

      for (const dateResult of result.dateResults) {
        if (!dateResult.hasData) {
          message += `📅 ${dateResult.date} - ⚪ No data\n`;
        } else if (dateResult.missingCount === 0) {
          message += `📅 ${dateResult.date} - ✅ All brands found\n`;
        } else {
          // Show all missing brands
          const missingList = dateResult.missingBrands.join(", ");
          message += `📅 ${dateResult.date} - ⚠️ Missing: ${missingList}\n`;
        }
      }

      message += `\n*Summary:* ${result.datesWithMissing} of ${result.totalDates} days with missing brands`;

      // Add duplicate check results
      if (result.duplicateCount > 0) {
        message += `\n\n🔴 *Duplicates Found:* ${result.duplicateCount} brand+date combinations\n`;
        // Show up to 10 duplicates
        const dupsToShow = result.duplicates.slice(0, 10);
        for (const dup of dupsToShow) {
          message += `• ${dup.date} - ${dup.brand} (${dup.count}x)\n`;
        }
        if (result.duplicates.length > 10) {
          message += `_...and ${result.duplicates.length - 10} more duplicates_\n`;
        }
      } else {
        message += `\n\n✅ *No duplicates found*`;
      }

      sendSlackResponse(responseUrl, message);

    } else {
      // Standard validation for non-BA Dash categories
      sendSlackResponse(responseUrl, `🔍 Starting brand validation for *${matchedCategory}* (Marketplace: ${marketplaceCode})...`);

      const result = AHA_ValidateBrands(matchedCategory, centralSheetId, marketplaceCode);

      if (!result.success) {
        sendSlackResponse(responseUrl, `❌ Validation failed: ${result.error}`);
        return;
      }

      if (result.skipped) {
        sendSlackResponse(responseUrl, `⚠️ Validation skipped: ${result.reason}`);
        return;
      }

      // Build standard response message
      let message = `📊 *Brand Validation Complete - ${matchedCategory}*\n\n`;
      message += `Marketplace: *${marketplaceCode}*\n`;
      message += `Expected Brands: *${result.expectedCount}*\n`;
      message += `Found Brands: *${result.foundCount}*\n`;
      message += `Missing Brands: *${result.missingCount}*\n`;

      if (result.missingCount > 0) {
        // Show all missing brands
        const missingList = result.missingBrands.join(", ");
        message += `\n*Missing:* ${missingList}`;
      } else {
        message += `\n✅ All brands found!`;
      }

      sendSlackResponse(responseUrl, message);
    }

  } catch (e) {
    sendSlackResponse(responseUrl, `❌ Error during validation: ${e.message}`);
  }
}


/**
 * Handles the /ibot help command - shows all available commands
 */
function handleHelpCommand(responseUrl) {
  const helpText = `📖 *iBot Command Reference*

*Import Commands:*
• \`/ibot [category]\` - Start import for a category (e.g., \`/ibot BA Produk SHO\`)

*Validation Commands:*
• \`/ibot validate [category]\` - Validate brands for a category
• \`/ibot validate [BA Dash category] [range]\` - Validate with date range + duplicate check (yesterday, L7D, L30D)
• \`/ibot validateall\` - Validate all categories at once

*Check Commands:*
• \`/ibot check move\` - Check files in Move folder
• \`/ibot check failed\` - Check files in Failed folder
• \`/ibot check other\` - Check files in other folders

*Info Commands:*
• \`/ibot status\` - System health check
• \`/ibot history\` - Recent import activities
• \`/ibot dashboard\` - Get dashboard link
• \`/ibot pingall\` - Ping all projects to check connectivity
• \`/ibot help\` - Show this help message
• \`/ibot test\` - Test bot connectivity`;

  sendSlackResponse(responseUrl, helpText);
}


/**
 * Handles the /ibot status command - shows system health
 */
function handleStatusCommand(responseUrl) {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const listSheet = ss.getSheetByName("List");
    const dashboardSheet = ss.getSheetByName("Dashboard");

    let message = `🔧 *iBot System Status*\n\n`;

    // Count active triggers
    const triggers = ScriptApp.getProjectTriggers();
    message += `*Active Triggers:* ${triggers.length}\n`;

    // Get category count from List sheet
    if (listSheet) {
      const categoryCount = listSheet.getLastRow() - 1;
      message += `*Categories Configured:* ${categoryCount}\n`;
    }

    // Check Dashboard for recent activity
    if (dashboardSheet) {
      // Find categories with recent imports (last 24 hours)
      const data = dashboardSheet.getRange("A6:C30").getValues();
      let recentImports = 0;
      let failedImports = 0;
      const now = new Date();
      const oneDayAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);

      for (const row of data) {
        const category = row[0];
        const lastImport = row[1];
        const status = row[2];

        if (category && lastImport instanceof Date && lastImport > oneDayAgo) {
          recentImports++;
          if (status === "Failed") failedImports++;
        }
      }

      message += `*Imports (Last 24h):* ${recentImports}\n`;
      if (failedImports > 0) {
        message += `*Failed Imports:* ${failedImports} ⚠️\n`;
      }
    }

    // Check for stuck files
    const ROOT_DRIVE_FOLDER_ID = '0AJyZWtXd1795Uk9PVA';
    try {
      const rootFolder = DriveApp.getFolderById(ROOT_DRIVE_FOLDER_ID);

      // Check Move folder
      const moveFolders = rootFolder.getFoldersByName('Move');
      let moveFileCount = 0;
      if (moveFolders.hasNext()) {
        const moveFolder = moveFolders.next();
        moveFileCount = countFilesRecursively(moveFolder);
      }

      // Check Failed folder
      const failedFolders = rootFolder.getFoldersByName('Failed');
      let failedFileCount = 0;
      if (failedFolders.hasNext()) {
        const failedFolder = failedFolders.next();
        failedFileCount = countFilesRecursively(failedFolder);
      }

      message += `\n*Pending Files:*\n`;
      message += `• Move folder: ${moveFileCount} files\n`;
      message += `• Failed folder: ${failedFileCount} files`;

      if (moveFileCount > 0 || failedFileCount > 0) {
        message += `\n\n💡 Use \`/ibot check move\` or \`/ibot check failed\` for details.`;
      }
    } catch (driveError) {
      message += `\n⚠️ Could not check Drive folders.`;
    }

    sendSlackResponse(responseUrl, message);

  } catch (e) {
    sendSlackResponse(responseUrl, `❌ Error getting status: ${e.message}`);
  }
}


/**
 * Helper function to count files recursively in a folder
 */
function countFilesRecursively(folder) {
  let count = 0;

  const files = folder.getFiles();
  while (files.hasNext()) {
    files.next();
    count++;
  }

  const subFolders = folder.getFolders();
  while (subFolders.hasNext()) {
    count += countFilesRecursively(subFolders.next());
  }

  return count;
}


/**
 * Handles the /ibot history command - shows recent import activities
 */
function handleHistoryCommand(responseUrl) {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const dashboardSheet = ss.getSheetByName("Dashboard");

    if (!dashboardSheet) {
      sendSlackResponse(responseUrl, "❌ Dashboard sheet not found. Run `AHA_SetupDashboard()` first.");
      return;
    }

    // Find the log section
    const allData = dashboardSheet.getRange("A1:E50").getValues();
    let logStartRow = -1;

    for (let i = 0; i < allData.length; i++) {
      if (allData[i][0] === "Recent Activity Log") {
        logStartRow = i + 2; // Skip header row
        break;
      }
    }

    if (logStartRow === -1) {
      sendSlackResponse(responseUrl, "❌ Activity log not found in Dashboard.");
      return;
    }

    let message = `📜 *Recent Activity (Last 10)*\n\n`;
    let activityCount = 0;

    for (let i = logStartRow; i < allData.length && activityCount < 10; i++) {
      const timestamp = allData[i][0];
      const category = allData[i][1];
      const action = allData[i][2];
      const details = allData[i][3];
      const status = allData[i][4];

      if (!timestamp || !category) continue;

      const timeStr = timestamp instanceof Date
        ? Utilities.formatDate(timestamp, "Asia/Jakarta", "dd/MM HH:mm")
        : timestamp;

      const statusIcon = status === "Success" ? "✅" : status === "Failed" ? "❌" : "⚠️";
      message += `${statusIcon} \`${timeStr}\` *${category}*\n    ${action}: ${details}\n`;
      activityCount++;
    }

    if (activityCount === 0) {
      message += "_No recent activity found._";
    }

    sendSlackResponse(responseUrl, message);

  } catch (e) {
    sendSlackResponse(responseUrl, `❌ Error getting history: ${e.message}`);
  }
}


/**
 * Handles the /ibot validateall command - validates all categories
 */
function handleValidateAllCommand(responseUrl) {
  try {
    sendSlackResponse(responseUrl, "🔍 Starting validation for all categories... This may take a few minutes.");

    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const listSheet = ss.getSheetByName("List");

    if (!listSheet) {
      sendSlackResponse(responseUrl, "❌ List sheet not found.");
      return;
    }

    // Get admin sheet for brand master
    const adminSS = SpreadsheetApp.openById(BRAND_VALIDATION_CONFIG.ADMIN_SHEET_ID);
    const brandMasterSheet = adminSS.getSheetByName("Brand Master");

    if (!brandMasterSheet) {
      sendSlackResponse(responseUrl, "❌ Brand Master sheet not found. Run `AHA_SetupDashboard()` first.");
      return;
    }

    // Get all categories from List sheet
    const listData = listSheet.getRange(2, 1, listSheet.getLastRow() - 1, 6).getValues();

    const results = [];
    let totalCategories = 0;
    let categoriesWithMissing = 0;
    let totalMissing = 0;

    for (const row of listData) {
      const category = (row[0] || "").toString().trim();
      const centralSheetId = row[5]; // Column F

      if (!category || !centralSheetId) continue;

      // Extract marketplace code
      const marketplaceCode = category.slice(-3).toUpperCase();
      if (!["SHO", "LAZ", "TIK", "TOK"].includes(marketplaceCode)) continue;

      totalCategories++;

      try {
        const isBADash = category.toLowerCase().startsWith("ba dash");
        let result;

        if (isBADash) {
          result = AHA_ValidateBADashBrands(category, centralSheetId, marketplaceCode, "L7D");
          if (result.success && !result.skipped) {
            let statusParts = [];
            if (result.datesWithMissing > 0) {
              categoriesWithMissing++;
              statusParts.push(`${result.datesWithMissing}/${result.totalDates} days missing`);
            }
            if (result.duplicateCount > 0) {
              statusParts.push(`${result.duplicateCount} dups`);
            }

            if (statusParts.length > 0) {
              results.push(`⚠️ *${category}*: ${statusParts.join(", ")}`);
            } else {
              results.push(`✅ *${category}*: All brands found, no duplicates (L7D)`);
            }
          } else if (result.skipped) {
            results.push(`⚪ *${category}*: Skipped - ${result.reason}`);
          }
        } else {
          result = AHA_ValidateBrands(category, centralSheetId, marketplaceCode);
          if (result.success && !result.skipped) {
            if (result.missingCount > 0) {
              categoriesWithMissing++;
              totalMissing += result.missingCount;
              results.push(`⚠️ *${category}*: ${result.missingCount} missing (${result.foundCount}/${result.expectedCount})`);
            } else {
              results.push(`✅ *${category}*: All ${result.expectedCount} brands found`);
            }
          } else if (result.skipped) {
            results.push(`⚪ *${category}*: Skipped - ${result.reason}`);
          }
        }
      } catch (catError) {
        results.push(`❌ *${category}*: Error - ${catError.message}`);
      }

      // Rate limit
      Utilities.sleep(500);
    }

    // Build summary message
    let message = `📊 *Validation Complete - All Categories*\n\n`;
    message += `*Summary:* ${totalCategories} categories checked\n`;
    message += `*Issues Found:* ${categoriesWithMissing} categories with missing brands\n\n`;
    message += `*Results:*\n`;
    message += results.join("\n");

    sendSlackResponse(responseUrl, message);

  } catch (e) {
    sendSlackResponse(responseUrl, `❌ Error during validation: ${e.message}`);
  }
}


/**
 * Handles the /ibot pingall command - pings all projects and reports their status
 */
function handlePingAllCommand(responseUrl) {
  try {
    sendSlackResponse(responseUrl, "🏓 Pinging all projects... Please wait.");

    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const listSheet = ss.getSheetByName("List");

    if (!listSheet) {
      sendSlackResponse(responseUrl, "❌ List sheet not found.");
      return;
    }

    // Get all projects from List sheet
    // Columns: A=Category/Type, B=Role, C=Link, D=Apps Script, E=Upload Folder, F=Spreadsheet ID, G=Apps Script Deployment URL
    const listData = listSheet.getRange(2, 1, listSheet.getLastRow() - 1, 7).getValues();

    const results = [];
    let onlineCount = 0;
    let offlineCount = 0;
    let totalProjects = 0;

    // Build array of ping requests
    const pingRequests = [];

    for (const row of listData) {
      const category = (row[0] || "").toString().trim();
      const role = (row[1] || "").toString().trim();
      const deploymentUrl = (row[6] || "").toString().trim(); // Column G (index 6)

      if (!category || !role || !deploymentUrl) continue;

      totalProjects++;
      pingRequests.push({
        category: category,
        role: role,
        url: deploymentUrl
      });
    }

    // Send ping requests (using fetchAll for parallel execution)
    const requests = pingRequests.map(project => ({
      url: project.url,
      method: 'post',
      contentType: 'application/json',
      payload: JSON.stringify({ command: 'ping' }),
      muteHttpExceptions: true
    }));

    // Execute all requests in parallel
    const responses = UrlFetchApp.fetchAll(requests);

    // Process responses
    for (let i = 0; i < responses.length; i++) {
      const project = pingRequests[i];
      const response = responses[i];
      const statusCode = response.getResponseCode();
      const responseText = response.getContentText().trim();

      if (statusCode === 200 && (responseText === 'PONG' || responseText.includes('Online'))) {
        onlineCount++;
        results.push(`${project.category} - ${project.role}: :green_ball: Online`);
      } else {
        offlineCount++;
        results.push(`${project.category} - ${project.role}: :red_circle: Offline (${statusCode})`);
      }
    }

    // Build summary message
    let message = `🏓 *Ping Results*\n\n`;
    message += `*Total:* ${totalProjects} projects\n`;
    message += `*Online:* ${onlineCount} :green_ball:\n`;
    message += `*Offline:* ${offlineCount} :red_circle:\n\n`;
    message += `*Details:*\n`;
    message += results.join("\n");

    sendSlackResponse(responseUrl, message);

  } catch (e) {
    sendSlackResponse(responseUrl, `❌ Error during ping: ${e.message}`);
  }
}
