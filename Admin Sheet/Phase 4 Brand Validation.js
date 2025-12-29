////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Phase 4 Brand Validation.js
// Validates imported brands against master brand list and alerts on missing brands
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Configuration - Edit these values as needed
 */
const BRAND_VALIDATION_CONFIG = {
  ADMIN_SHEET_ID: "11aYJSWTW7xxZcyfREdcvGoUhgPnl4xU8MQM6lE1se4M",
  SLACK_WEBHOOK_URL: "", // Will be set from script properties
  ADMIN_USER_TAG: "<@U08TUF8LW2H>"
};


/**
 * Main validation function - Called after import completes
 * Compares imported brands against master list and reports missing ones
 *
 * @param {string} category - The category that was just imported
 * @param {string} centralSheetId - The ID of the Central sheet containing imported data
 * @param {string} marketplaceCode - The marketplace code (SHO, LAZ, TIK, TOK)
 */
function AHA_ValidateBrands(category, centralSheetId, marketplaceCode) {
  try {
    Logger.log(`Starting brand validation for ${category} (Marketplace: ${marketplaceCode})`);

    // Get the Admin sheet for brand master list
    const adminSS = SpreadsheetApp.openById(BRAND_VALIDATION_CONFIG.ADMIN_SHEET_ID);
    const brandMasterSheet = adminSS.getSheetByName("Brand Master");

    if (!brandMasterSheet) {
      Logger.log("Brand Master sheet not found in Admin Sheet");
      return { success: false, error: "Brand Master sheet not found" };
    }

    // Get expected brands from Brand Master, filtered by marketplace code
    const expectedBrands = getBrandMasterList(brandMasterSheet, marketplaceCode);
    if (expectedBrands.length === 0) {
      Logger.log(`No brands found in Brand Master for marketplace ${marketplaceCode}. Skipping validation.`);
      return { success: true, skipped: true, reason: `No brands for ${marketplaceCode} in master list` };
    }

    // Get imported brands from the Central sheet's category tab
    const centralSS = SpreadsheetApp.openById(centralSheetId);
    const categorySheet = centralSS.getSheetByName(category);

    if (!categorySheet) {
      Logger.log(`Category sheet '${category}' not found in Central sheet`);
      return { success: false, error: `Category sheet '${category}' not found` };
    }

    // Get brands from Column A (skip header row)
    const importedBrands = getImportedBrands(categorySheet);
    Logger.log(`Found ${importedBrands.size} unique brands in imported data`);

    // Compare and find missing brands
    const missingBrands = findMissingBrands(expectedBrands, importedBrands);
    Logger.log(`Missing brands: ${missingBrands.length}`);

    // Update Brand Validation sheet
    updateBrandValidationSheet(adminSS, category, expectedBrands.length, importedBrands.size, missingBrands);

    // Update Dashboard
    const status = missingBrands.length === 0 ? "Success" : "Warning";
    AHA_UpdateDashboardStatus(category, status, categorySheet.getLastRow() - 1, importedBrands.size, missingBrands.length);

    // Log activity
    const details = missingBrands.length === 0
      ? `All ${expectedBrands.length} brands found`
      : `${missingBrands.length} brands missing`;
    AHA_LogActivity(category, "Brand Validation", details, status);

    // Send Slack alert if brands are missing
    if (missingBrands.length > 0) {
      sendMissingBrandsAlert(category, expectedBrands.length, importedBrands.size, missingBrands);
    }

    return {
      success: true,
      expectedCount: expectedBrands.length,
      foundCount: importedBrands.size,
      missingCount: missingBrands.length,
      missingBrands: missingBrands
    };

  } catch (e) {
    Logger.log(`Error in brand validation: ${e.message}`);
    return { success: false, error: e.message };
  }
}


/**
 * Gets the master brand list from the Brand Master sheet, filtered by marketplace code
 * Brand Master structure: Column A = Marketplace Code, Column B = Brand Code
 * Data starts at row 10 (row 9 is header)
 *
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet - The Brand Master sheet
 * @param {string} marketplaceCode - The marketplace code to filter by (SHO, LAZ, TIK, TOK)
 * @returns {string[]} Array of brand codes for the specified marketplace
 */
function getBrandMasterList(sheet, marketplaceCode) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 10) return []; // No data yet (header is at row 9)

  // Get both columns: A (Marketplace Code) and B (Brand Code)
  const data = sheet.getRange(10, 1, lastRow - 9, 2).getValues();
  const brands = [];

  for (const row of data) {
    const rowMarketplace = (row[0] || "").toString().trim().toUpperCase();
    const brandCode = (row[1] || "").toString().trim();

    // Filter by marketplace code and skip placeholder/example rows
    if (rowMarketplace === marketplaceCode.toUpperCase() &&
        brandCode !== "" &&
        !brandCode.startsWith("(")) {
      brands.push(brandCode.toUpperCase()); // Normalize to uppercase for comparison
    }
  }

  Logger.log(`Found ${brands.length} brands for marketplace ${marketplaceCode}`);
  return brands;
}


/**
 * Gets unique brands from imported data (Column A)
 */
function getImportedBrands(sheet) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return new Set(); // No data

  const data = sheet.getRange(2, 1, lastRow - 1, 1).getValues();
  const brands = new Set();

  for (const row of data) {
    const brand = (row[0] || "").toString().trim();
    if (brand !== "") {
      brands.add(brand.toUpperCase()); // Normalize to uppercase
    }
  }

  return brands;
}


/**
 * Finds brands that are expected but not in imported data
 */
function findMissingBrands(expectedBrands, importedBrandsSet) {
  const missing = [];

  for (const brand of expectedBrands) {
    if (!importedBrandsSet.has(brand)) {
      missing.push(brand);
    }
  }

  return missing;
}


/**
 * Updates the Brand Validation sheet with results
 */
function updateBrandValidationSheet(ss, category, expectedCount, foundCount, missingBrands) {
  const sheet = ss.getSheetByName("Brand Validation");
  if (!sheet) return;

  // Find the category row
  const data = sheet.getRange("A5:A30").getValues();
  for (let i = 0; i < data.length; i++) {
    if (data[i][0] === category) {
      const row = 5 + i;
      sheet.getRange(row, 2).setValue(expectedCount); // Expected Brands
      sheet.getRange(row, 3).setValue(foundCount); // Found Brands
      sheet.getRange(row, 4).setValue(missingBrands.length); // Missing Count
      sheet.getRange(row, 5).setValue(missingBrands.length > 0 ? missingBrands.join(", ") : "-"); // Missing Brands
      sheet.getRange(row, 6).setValue(new Date()); // Last Checked
      break;
    }
  }
}


/**
 * Sends a Slack alert for missing brands
 */
function sendMissingBrandsAlert(category, expectedCount, foundCount, missingBrands) {
  const webhookUrl = PropertiesService.getScriptProperties().getProperty("SLACK_WEBHOOK_URL");
  if (!webhookUrl) {
    Logger.log("Slack webhook URL not configured");
    return;
  }

  const missingList = missingBrands.length <= 10
    ? missingBrands.join(", ")
    : missingBrands.slice(0, 10).join(", ") + ` ... and ${missingBrands.length - 10} more`;

  const message = {
    text: `⚠️ *Brand Validation Alert* - *${category}*\n\n` +
          `Expected: *${expectedCount}* brands\n` +
          `Found: *${foundCount}* brands\n` +
          `Missing: *${missingBrands.length}* brands\n\n` +
          `*Missing Brands:*\n${missingList}\n\n` +
          `${BRAND_VALIDATION_CONFIG.ADMIN_USER_TAG}`
  };

  const options = {
    method: "post",
    contentType: "application/json",
    payload: JSON.stringify(message),
    muteHttpExceptions: true
  };

  try {
    UrlFetchApp.fetch(webhookUrl, options);
    Logger.log("Sent missing brands alert to Slack");
  } catch (e) {
    Logger.log(`Error sending Slack alert: ${e.message}`);
  }
}


/**
 * Manual test function - Run this to test brand validation
 * Set the category and Central Sheet ID before running
 */
function testBrandValidation() {
  const testCategory = "BA Produk SHO"; // Change as needed
  const testCentralSheetId = "YOUR_CENTRAL_SHEET_ID"; // Replace with actual ID
  const testMarketplaceCode = "SHO"; // Extracted from category name

  const result = AHA_ValidateBrands(testCategory, testCentralSheetId, testMarketplaceCode);
  Logger.log(JSON.stringify(result, null, 2));
}


/**
 * Validates brands for ALL categories
 * Useful for a daily scheduled check
 */
function AHA_ValidateAllBrands() {
  // Get Central Sheet IDs from List sheet
  // List columns: A: Category, F: Spreadsheet ID (index 5)
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const listSheet = ss.getSheetByName("List");
  if (!listSheet) {
    Logger.log("List sheet not found");
    return;
  }

  const listData = listSheet.getRange(2, 1, listSheet.getLastRow() - 1, 6).getValues();

  // Validate each category
  let totalMissing = 0;
  const results = [];

  for (const row of listData) {
    const category = (row[0] || "").toString().trim();
    const centralId = row[5]; // Column F = Spreadsheet ID

    if (!category || !centralId) continue;

    // Extract marketplace code from category name (last 3 characters)
    const marketplaceCode = category.slice(-3).toUpperCase();
    if (!["SHO", "LAZ", "TIK", "TOK"].includes(marketplaceCode)) {
      Logger.log(`Skipping ${category} - no valid marketplace code`);
      continue;
    }

    try {
      const result = AHA_ValidateBrands(category, centralId, marketplaceCode);
      if (result.success && result.missingCount > 0) {
        totalMissing += result.missingCount;
        results.push(`${category}: ${result.missingCount} missing`);
      }
    } catch (e) {
      Logger.log(`Error validating ${category}: ${e.message}`);
    }

    // Rate limit protection
    Utilities.sleep(1000);
  }

  // Send summary to Slack if there are missing brands
  if (totalMissing > 0) {
    sendValidationSummary(results, totalMissing);
  } else {
    Logger.log("All categories validated - no missing brands");
  }
}


/**
 * Sends a validation summary to Slack
 */
function sendValidationSummary(results, totalMissing) {
  const webhookUrl = PropertiesService.getScriptProperties().getProperty("SLACK_WEBHOOK_URL");
  if (!webhookUrl) return;

  const message = {
    text: `📊 *Daily Brand Validation Summary*\n\n` +
          `Total Missing Brands: *${totalMissing}*\n\n` +
          `*Details:*\n• ${results.join("\n• ")}\n\n` +
          `${BRAND_VALIDATION_CONFIG.ADMIN_USER_TAG}`
  };

  const options = {
    method: "post",
    contentType: "application/json",
    payload: JSON.stringify(message),
    muteHttpExceptions: true
  };

  UrlFetchApp.fetch(webhookUrl, options);
}
