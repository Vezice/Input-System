////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Importing.gs 
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// VSCode confirmed
// Synchronized
// Rangga was Here
// Man Down -> by Alif Masyhur
// Wah

/**
 * Utility function to convert a column number (e.g., 1, 27) to its corresponding letter (e.g., A, AA).
 * @param {number} col The column number.
 * @returns {string} The column letter.
 */
function AHA_LetterToNumber2(col) {
  const start = new Date();
  try {
    let temp, letter = '';
    // Loop to calculate the letter representation based on powers of 26.
    while (col > 0) {
      temp = (col - 1) % 26;
      letter = String.fromCharCode(temp + 65) + letter;
      col = (col - temp - 1) / 26;
    }
    return letter;

  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}

/**
 * Helper function to get a subfolder within a parent folder by name, or create it if it doesn't exist.
 * @param {GoogleAppsScript.Drive.Folder} parent The parent folder.
 * @param {string} name The name of the subfolder to find or create.
 * @returns {GoogleAppsScript.Drive.Folder} The subfolder object.
 */
function AHA_GetSubFolder2(parent, name) {
  const start = new Date();
  try {
    // Check if a folder with the given name already exists.
    const folders = parent.getFoldersByName(name);
    // If it exists, return it. Otherwise, create a new one.
    return folders.hasNext() ? folders.next() : parent.createFolder(name);

  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}

/**
 * Sorts the validation results on the "Input" sheet.
 * The primary sort key is Category (Column E), and the secondary is Folder Name (Column B).
 */
function AHA_SortValidationList2() {
  const start = new Date();
  try {

    const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Input");
    const startRow = 5;
    const startCol = 2;
    const numCols = 5;

    // Fetch all possible data rows to ensure the clear operation covers everything.
    const allData = sheet.getRange(startRow, startCol, sheet.getMaxRows() - startRow + 1, numCols).getValues();

    // Filter out completely empty rows to only sort actual data.
    const data = allData.filter(row => row.some(cell => cell !== "" && cell != null));
    if (data.length === 0) {
      Logger.log("No data to sort.");
      AHA_SlackNotify3("❌ *Error*: No data to sort. <@U0A6B24777X>");
      return;
    }

    // Sort the data array. First by Category (index 3), then by Folder Name (index 0).
    data.sort((a, b) => {
      const catCompare = a[3].localeCompare(b[3]); // Compare categories.
      // If categories are the same (catCompare is 0), then compare by folder name.
      return catCompare !== 0 ? catCompare : a[0].localeCompare(b[0]);
    });

    // Clear the entire original data range to prevent leftover rows if the new data is shorter.
    sheet.getRange(startRow, startCol, allData.length, numCols).clearContent();

    // Write the newly sorted data back to the sheet in one operation.
    sheet.getRange(startRow, startCol, data.length, numCols).setValues(data);

    Logger.log("✅ Validation results sorted by category and folder name.");
    AHA_SlackNotify3("✅ *Completed* : Validation results sorted by category and folder name.");

  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}

/**
 * Archives processed files from their source folders into a structured "Archive" directory.
 * The archive structure is /Archive/[Category]/[Date]/[file].
 * -- MODIFIED to use batched Slack notifications to avoid rate limiting. --
 */
function AHA_ArchiveFilesByCategory2() {
  const start = new Date();
  try {
    // --- SETUP ---
    const ROOT_DRIVE_ID = "0AJyZWtXd1795Uk9PVA";
    const EXCLUDED_FOLDERS = ["Failed", "Move", "Archive"];
    const root = DriveApp.getFolderById(ROOT_DRIVE_ID);
    const archiveRoot = AHA_GetSubFolder2(root, "Archive");
    const dateStamp = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd");

    const inputSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Input");
    const lastRow = inputSheet.getLastRow();
    
    const folderNames = inputSheet.getRange("B5:B" + lastRow).getValues().flat().filter(name => name);
    const fileNames = inputSheet.getRange("C5:C" + lastRow).getValues().flat().filter(name => name);

    const foldersToArchive = new Set(folderNames);
    const filesToArchive = new Set(fileNames);

    const categoryMap = AHA_GetCategoryFromSheet2();
    const folders = root.getFolders();

    // --- NEW: Arrays to hold the results for batch notification ---
    const successfulArchives = [];
    const failedArchives = [];

    // --- MAIN ARCHIVING LOOP ---
    while (folders.hasNext()) {
      const sourceFolder = folders.next();
      const folderName = sourceFolder.getName();

      if (EXCLUDED_FOLDERS.includes(folderName) || !foldersToArchive.has(folderName)) {
        continue;
      }

      const files = sourceFolder.getFiles();
      while (files.hasNext()) {
        const file = files.next();
        const oldName = file.getName();

        if (!filesToArchive.has(oldName)) {
          continue;
        }

        const fileId = file.getId();
        const category = categoryMap[folderName] || "Uncategorized";
        const categoryFolder = AHA_GetSubFolder2(archiveRoot, category);
        const datedFolder = AHA_GetSubFolder2(categoryFolder, dateStamp);

        try {
          const parentIds = [];
          const parents = file.getParents();
          while (parents.hasNext()) {
            parentIds.push(parents.next().getId());
          }

          Drive.Files.update(
            { title: oldName }, // Keep original name
            fileId,
            null, {
              supportsAllDrives: true,
              addParents: datedFolder.getId(),
              removeParents: parentIds.join(",")
            }
          );
          
          // --- MODIFICATION: Instead of sending a notification, add to the success list ---
          successfulArchives.push(oldName);
          Logger.log(`✅ Archived: ${oldName}`);

        } catch (err) {
          // --- MODIFICATION: Instead of sending a notification, add to the failure list ---
          const errorMessage = `Failed to archive ${oldName}: ${err}`;
          failedArchives.push(errorMessage);
          Logger.log(`❌ ${errorMessage}`);
        }
      }
    }

    // --- NEW: Send one summary notification at the end ---
    let summaryMessage = "";

    if (successfulArchives.length > 0) {
      summaryMessage += `✅ *Archiving Complete*\n*Successfully archived ${successfulArchives.length} files.*\n`;
      // To keep the message clean, you might only list the first few or omit the list entirely
      // For example: summaryMessage += "Including: " + successfulArchives.slice(0, 5).join(", ") + "\n";
    }

    if (failedArchives.length > 0) {
      summaryMessage += `❌ *${failedArchives.length} files failed to archive.*\n`;
      // Join the detailed error messages for the report
      summaryMessage += "```\n" + failedArchives.join("\n") + "\n```"; 
      summaryMessage += "<@U0A6B24777X>"; // Mention user on failure
    }

    if (summaryMessage) {
        AHA_SlackNotify3(summaryMessage);
    } else {
        AHA_SlackNotify3("✅ Archiving complete. No new files to process.");
    }
    
  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}

/**
 * Helper function that reads the "Input" sheet to create a mapping
 * of folder names (brands/accounts) to their corresponding category.
 * @returns {Object} A map where keys are folder names and values are category names.
 */
function AHA_GetCategoryFromSheet2() {
  const start = new Date();
  try {

    const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Input");
    const data = sheet.getRange("B5:F").getValues(); // FolderName, FileName, Status, Category, Validation
    const map = {};
    // Loop through the data to build the map.
    data.forEach(row => {
      const [folder, , , category] = row;
      // If the folder and category exist and we haven't already mapped this folder, add it.
      if (folder && category && !map[folder]) {
        map[folder] = category;
      }
    });
    return map;

  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}















