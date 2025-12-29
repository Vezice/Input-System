/**
 * Renames files in a specific Drive folder by prepending codes from a Google Sheet.
 *
 * This function reads a list of codes from a sheet named "Test Input TIK TOK".
 * It then iterates through files in a specified Drive folder and renames them
 * in the format: "[MP Value] [Kode Akun Value] [Original File Name]".
 * The process stops when either the list of codes or the list of files runs out.
 */
function renameFilesWithCodes() {
  // --- CONFIGURATION ---
  // IMPORTANT: Replace with the ID of your Google Drive folder.
  const FOLDER_ID = "1q1CluJaYHdfNVCLY0gur7mN8lhOWV8q8"; 
  // Upload Here BA Dash TIK : https://drive.google.com/drive/folders/13l41Xj_bpjBg_Syh6n0tOcDuLc9vPyCM
  // Upload Here BA Dash TOK : https://drive.google.com/drive/folders/1q1CluJaYHdfNVCLY0gur7mN8lhOWV8q8

  const SHEET_NAME = "Test Input TIK TOK";
  const MP_COLUMN_NAME = "MP";
  const KODE_AKUN_COLUMN_NAME = "Kode Akun";
  // --------------------

  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = ss.getSheetByName(SHEET_NAME);

    if (!sheet) {
      throw new Error(`Sheet named "${SHEET_NAME}" was not found.`);
    }

    // 1. Get the codes from the spreadsheet.
    const data = sheet.getDataRange().getValues();
    const headers = data.shift(); // Get and remove the header row

    const mpColIndex = headers.indexOf(MP_COLUMN_NAME);
    const kodeAkunColIndex = headers.indexOf(KODE_AKUN_COLUMN_NAME);

    if (mpColIndex === -1 || kodeAkunColIndex === -1) {
      throw new Error(`Could not find the required columns: "${MP_COLUMN_NAME}" and/or "${KODE_AKUN_COLUMN_NAME}".`);
    }

    // Create a clean list of codes, filtering out any empty rows.
    const codes = data.map(row => ({
      mp: row[mpColIndex],
      kodeAkun: row[kodeAkunColIndex]
    })).filter(code => code.mp && code.kodeAkun);

    if (codes.length === 0) {
      Logger.log("No valid codes found in the sheet. Halting process.");
      return;
    }

    // 2. Get the files from the Google Drive folder.
    const folder = DriveApp.getFolderById(FOLDER_ID);
    const files = folder.getFiles();
    let filesRenamed = 0;

    Logger.log(`Found ${codes.length} codes in the sheet. Starting file renaming process...`);

    // 3. Loop through the codes and rename files sequentially.
    for (const code of codes) {
      if (!files.hasNext()) {
        Logger.log("No more files left to rename. The process is complete.");
        break; // Exit the loop if we run out of files
      }

      const file = files.next();
      const originalName = file.getName();
      
      // Construct the new file name.
      const newName = `${code.mp} ${code.kodeAkun} ${originalName}`;

      try {
        file.setName(newName);
        Logger.log(`Renamed "${originalName}" to "${newName}"`);
        filesRenamed++;
      } catch (renameError) {
        Logger.log(`Could not rename "${originalName}": ${renameError.message}`);
      }
    }

    Logger.log(`✅ Process complete. Renamed a total of ${filesRenamed} file(s).`);

  } catch (e) {
    Logger.log(`❌ A fatal error occurred: ${e.message}`);
    // Optional: Show an alert to the user in the spreadsheet UI.
    // SpreadsheetApp.getUi().alert(`An error occurred: ${e.message}`);
  }
}

/**
 * Renames files in a specific Drive folder by prepending codes from a Google Sheet.
 *
 * This function reads a list of codes from a sheet named "Test Input TIK TOK".
 * It then iterates through files in a specified Drive folder and renames them
 * in the format: "[MP Value] [Kode Akun Value] [Original File Name]".
 * The process stops when either the list of codes or the list of files runs out.
 */
function renameFilesWithCodesLAZ() {
  // --- CONFIGURATION ---
  // IMPORTANT: Replace with the ID of your Google Drive folder.
  const FOLDER_ID = "1EYnk5uwBSwAa1D3R3J2T5QTcwg0ATywE"; 
  // Upload Here BA Dash LAZ : https://drive.google.com/drive/folders/1EYnk5uwBSwAa1D3R3J2T5QTcwg0ATywE

  const SHEET_NAME = "Test Input LAZ";
  const KODE_AKUN_COLUMN_NAME = "Kode Akun";
  // --------------------

  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = ss.getSheetByName(SHEET_NAME);

    if (!sheet) {
      throw new Error(`Sheet named "${SHEET_NAME}" was not found.`);
    }

    // 1. Get the codes from the spreadsheet.
    const data = sheet.getDataRange().getValues();
    const headers = data.shift(); // Get and remove the header row

    const kodeAkunColIndex = headers.indexOf(KODE_AKUN_COLUMN_NAME);

    if (kodeAkunColIndex === -1) {
      throw new Error(`Could not find the required columns: "${KODE_AKUN_COLUMN_NAME}".`);
    }

    // Create a clean list of codes, filtering out any empty rows.
    const codes = data.map(row => ({
      kodeAkun: row[kodeAkunColIndex]
    })).filter(code => code.kodeAkun);

    if (codes.length === 0) {
      Logger.log("No valid codes found in the sheet. Halting process.");
      return;
    }

    // 2. Get the files from the Google Drive folder.
    const folder = DriveApp.getFolderById(FOLDER_ID);
    const files = folder.getFiles();
    let filesRenamed = 0;

    Logger.log(`Found ${codes.length} codes in the sheet. Starting file renaming process...`);

    // 3. Loop through the codes and rename files sequentially.
    for (const code of codes) {
      if (!files.hasNext()) {
        Logger.log("No more files left to rename. The process is complete.");
        break; // Exit the loop if we run out of files
      }

      const file = files.next();
      const originalName = file.getName();
      
      // Construct the new file name.
      const newName = `${code.kodeAkun} ${originalName}`;

      try {
        file.setName(newName);
        Logger.log(`Renamed "${originalName}" to "${newName}"`);
        filesRenamed++;
      } catch (renameError) {
        Logger.log(`Could not rename "${originalName}": ${renameError.message}`);
      }
    }

    Logger.log(`✅ Process complete. Renamed a total of ${filesRenamed} file(s).`);

  } catch (e) {
    Logger.log(`❌ A fatal error occurred: ${e.message}`);
    // Optional: Show an alert to the user in the spreadsheet UI.
    // SpreadsheetApp.getUi().alert(`An error occurred: ${e.message}`);
  }
}
