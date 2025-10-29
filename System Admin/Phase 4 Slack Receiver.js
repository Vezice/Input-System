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
  if (!responseUrl) {
    Logger.log("Cannot send Slack response: response_url is missing.");
    return;
  }
  const payload = {
    response_type: "in_channel", 
    text: "System Admin: "+text
  };
  const options = {
    method: "post",
    contentType: "application/json",
    payload: JSON.stringify(payload)
  };
  UrlFetchApp.fetch(responseUrl, options);
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



