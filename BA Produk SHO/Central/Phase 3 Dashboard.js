////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Dashboard.gs 
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Fino
// Rangga is Here
// Rangga was Here

// Helper Function
function AHA_UpdateCellUsage2() { // updateCellUsageStats()
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheets = ss.getSheets();
  const limit = 10000000;

  let usedCells = 0;

  sheets.forEach(sheet => {
    const range = sheet.getDataRange();
    const numRows = range.getNumRows();
    const numCols = range.getNumColumns();
    usedCells += numRows * numCols;
  });

  const remainingCells = limit - usedCells;

  const summarySheet = ss.getSheetByName("System Dashboard");
  const now = new Date();

  // Write values
  summarySheet.getRange("C10").setValue(usedCells);
  summarySheet.getRange("D10").setValue(now);
  summarySheet.getRange("C11").setValue(remainingCells);
  summarySheet.getRange("D11").setValue(now);

  Logger.log(`Used Cells: ${usedCells}, Remaining: ${remainingCells}`);
  AHA_SlackNotify3("⚠️ Total Cells Used: " + usedCells + ", Remaining: " + remainingCells);
}


function AHA_UpdateTotalFilesProcessed3() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const inputSheet = ss.getSheetByName("Input");
  const dashboard = ss.getSheetByName("System Dashboard");

  if (!inputSheet || !dashboard) {
    throw new Error("❌ Required sheet not found.");
  }

  // Assume value is in B1, timestamp goes in C1
  const totalCell = dashboard.getRange("C5");
  const timestampCell = dashboard.getRange("D5");

  const currentValue = Number(totalCell.getValue()) || 0;

  // Count how many rows in column C (status) are "Added"
  const fileStatusCol = inputSheet.getRange("D5:D" + inputSheet.getLastRow()).getValues().flat();
  const newProcessed = fileStatusCol.filter(value => value === "Added").length;

  // Update total
  const updatedTotal = currentValue + newProcessed;
  totalCell.setValue(updatedTotal);

  // Log timestamp
  const now = new Date();
  timestampCell.setValue(now);

  Logger.log(`✅ Total Files Processed updated to ${updatedTotal} on ${now.toLocaleString()}`);
}


function AHA_UpdateCategoriesImported3() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const inputSheet = ss.getSheetByName("Input");
  const dashboard = ss.getSheetByName("System Dashboard");

  if (!inputSheet || !dashboard) {
    throw new Error("❌ Required sheet not found.");
  }

  // Get all values in column E (starting from row 5)
  const categoryValues = inputSheet.getRange("E5:E" + inputSheet.getLastRow()).getValues().flat();
  const uniqueCategories = [...new Set(categoryValues.filter(Boolean))]; // Remove empty & deduplicate

  // Write to Dashboard
  dashboard.getRange("C6").setValue(uniqueCategories.length);
  dashboard.getRange("D6").setValue(new Date());

  Logger.log(`✅ Categories Imported: ${uniqueCategories.length}`);
}

function AHA_UpdateTriggerAndRuntimeStats3() {
  const dashboard = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("System Dashboard");
  if (!dashboard) throw new Error("❌ System Dashboard sheet not found.");

  const now = new Date();
  const timezone = SpreadsheetApp.getActive().getSpreadsheetTimeZone();

  // DAILY TRIGGER EXECUTIONS (estimate using logs)
  const triggersToday = ScriptApp.getProjectTriggers().filter(trigger => {
    const time = trigger.getTriggerSource() === ScriptApp.TriggerSource.CLOCK ? trigger.getUniqueId() : null;
    return time;
  }).length;

  // TOTAL RUNTIME USED TODAY (approximate, using Properties log)
  const scriptProperties = PropertiesService.getScriptProperties();
  const runtimeUsedToday = Number(scriptProperties.getProperty("RUNTIME_USED_TODAY") || 0); // You must track this manually

  // ACTIVE TRIGGERS
  const activeTriggers = ScriptApp.getProjectTriggers().length;

  // Write to Dashboard
  dashboard.getRange("C7").setValue(triggersToday);
  dashboard.getRange("D7").setValue(now);

  dashboard.getRange("C8").setValue(runtimeUsedToday);
  dashboard.getRange("D8").setValue(now);

  // dashboard.getRange("C9").setValue(runtimeQuota);
  dashboard.getRange("D9").setValue(now);

  dashboard.getRange("C12").setValue(activeTriggers);
  dashboard.getRange("D12").setValue(now);

  Logger.log("✅ Dashboard stats updated.");
}

function AHA_LogRuntime3(durationInMs) {
  const props = PropertiesService.getScriptProperties();
  const current = Number(props.getProperty("RUNTIME_USED_TODAY") || 0);
  props.setProperty("RUNTIME_USED_TODAY", current + Math.floor(durationInMs / 1000)); // Save in seconds
}

// Run this at midnight everyday
function AHA_ResetDailyRuntime3() {
  const props = PropertiesService.getScriptProperties();
  props.setProperty("RUNTIME_USED_TODAY", "0");

  const dashboard = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Dashboard");
  if (dashboard) {
    dashboard.getRange("C8").setValue(0);
    dashboard.getRange("D8").setValue(new Date());
  }

  Logger.log("✅ Daily runtime counter has been reset.");
}