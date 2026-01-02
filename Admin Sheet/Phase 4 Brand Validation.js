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
  ADMIN_USER_TAG: "<@U0A6B24777X>"
};

/**
 * BA Dash date column mapping (in "Import" sheet)
 * Column A = Brand Code for all
 * Date columns vary by category
 */
const BA_DASH_DATE_COLUMNS = {
  "BA Dash SHO": 18, // Column R
  "BA Dash LAZ": 2,  // Column B
  "BA Dash TIK": 2,  // Column B
  "BA Dash TOK": 2   // Column B
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
 * Validates BSL categories against ALL unique brands from ALL marketplaces.
 * BSL contains data for all brands across SHO, LAZ, TIK, TOK.
 *
 * @param {string} category - The BSL category (e.g., "Demografis BSL", "Proyeksi Stok BSL")
 * @param {string} centralSheetId - The ID of the Central sheet
 * @returns {Object} Validation results
 */
function AHA_ValidateBSLBrands(category, centralSheetId) {
  try {
    Logger.log(`Starting BSL brand validation for ${category}`);

    // Get the Admin sheet for brand master list
    const adminSS = SpreadsheetApp.openById(BRAND_VALIDATION_CONFIG.ADMIN_SHEET_ID);
    const brandMasterSheet = adminSS.getSheetByName("Brand Master");

    if (!brandMasterSheet) {
      Logger.log("Brand Master sheet not found in Admin Sheet");
      return { success: false, error: "Brand Master sheet not found" };
    }

    // Get ALL unique brands from ALL marketplaces (SHO, LAZ, TIK, TOK)
    const expectedBrands = getBrandMasterListAllMarketplaces(brandMasterSheet);
    if (expectedBrands.length === 0) {
      Logger.log("No brands found in Brand Master. Skipping validation.");
      return { success: true, skipped: true, reason: "No brands in master list" };
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

    return {
      success: true,
      expectedCount: expectedBrands.length,
      foundCount: importedBrands.size,
      missingCount: missingBrands.length,
      missingBrands: missingBrands
    };

  } catch (e) {
    Logger.log(`Error in BSL brand validation: ${e.message}`);
    return { success: false, error: e.message };
  }
}


/**
 * Validates BA Dash categories with date-based filtering
 * Uses "Import" sheet which has clean, processed data with fixed date format
 *
 * @param {string} category - The BA Dash category (e.g., "BA Dash SHO")
 * @param {string} centralSheetId - The ID of the Central sheet
 * @param {string} marketplaceCode - The marketplace code (SHO, LAZ, TIK, TOK)
 * @param {string} dateRange - Date range: "yesterday", "L7D", "L30D" (default: "L7D")
 * @returns {Object} Validation results with per-date breakdown
 */
function AHA_ValidateBADashBrands(category, centralSheetId, marketplaceCode, dateRange) {
  try {
    dateRange = dateRange || "L7D";
    Logger.log(`Starting BA Dash validation for ${category} (${dateRange})`);
    Logger.log(`Central Sheet ID: ${centralSheetId}`);

    // Get expected brands from Brand Master
    const adminSS = SpreadsheetApp.openById(BRAND_VALIDATION_CONFIG.ADMIN_SHEET_ID);
    const brandMasterSheet = adminSS.getSheetByName("Brand Master");

    if (!brandMasterSheet) {
      return { success: false, error: "Brand Master sheet not found" };
    }

    const expectedBrands = getBrandMasterList(brandMasterSheet, marketplaceCode);
    if (expectedBrands.length === 0) {
      return { success: true, skipped: true, reason: `No brands for ${marketplaceCode} in master list` };
    }

    // Open Central Sheet's Import tab (contains clean, processed data)
    const centralSS = SpreadsheetApp.openById(centralSheetId);

    // Debug: Log all sheet names in this spreadsheet
    const allSheets = centralSS.getSheets().map(s => s.getName());
    Logger.log(`Sheets in Central: ${allSheets.join(", ")}`);

    const importSheet = centralSS.getSheetByName("Import");

    if (!importSheet) {
      return { success: false, error: `Import sheet not found. Available sheets: ${allSheets.join(", ")}` };
    }

    // Get date column for this category
    const dateColumn = BA_DASH_DATE_COLUMNS[category];
    if (!dateColumn) {
      return { success: false, error: `No date column mapping for ${category}` };
    }

    // Calculate date range
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    let startDate, endDate;

    if (dateRange === "yesterday") {
      startDate = new Date(today);
      startDate.setDate(startDate.getDate() - 1);
      endDate = new Date(startDate);
    } else if (dateRange === "L7D") {
      endDate = new Date(today);
      endDate.setDate(endDate.getDate() - 1); // Yesterday
      startDate = new Date(endDate);
      startDate.setDate(startDate.getDate() - 6); // 7 days back from yesterday
    } else if (dateRange === "L30D") {
      endDate = new Date(today);
      endDate.setDate(endDate.getDate() - 1); // Yesterday
      startDate = new Date(endDate);
      startDate.setDate(startDate.getDate() - 29); // 30 days back from yesterday
    } else {
      return { success: false, error: `Invalid date range: ${dateRange}` };
    }

    Logger.log(`Date range: ${startDate.toDateString()} to ${endDate.toDateString()}`);

    // Get data from Import sheet (Column A = Brand, dateColumn = Date)
    const lastRow = importSheet.getLastRow();
    if (lastRow < 2) {
      return { success: false, error: "No data in Import sheet" };
    }

    // Get brand column (A) and date column
    const maxCol = Math.max(1, dateColumn);
    const data = importSheet.getRange(2, 1, lastRow - 1, maxCol).getValues();

    // Group brands by date (for missing brand check)
    // Also track brand+date counts (for duplicate check)
    const brandsByDate = {};
    const brandDateCounts = {}; // Key: "date|brand", Value: count

    for (const row of data) {
      const brand = (row[0] || "").toString().trim().toUpperCase();
      const dateValue = row[dateColumn - 1]; // Convert to 0-based index

      if (!brand || !dateValue) continue;

      // Parse date (handle both Date objects and strings like "31 Dec 2025")
      let rowDate;
      if (dateValue instanceof Date) {
        rowDate = new Date(dateValue);
      } else {
        rowDate = new Date(dateValue);
      }

      if (isNaN(rowDate.getTime())) continue; // Skip invalid dates

      rowDate.setHours(0, 0, 0, 0);

      // Check if date is within range
      if (rowDate >= startDate && rowDate <= endDate) {
        const dateKey = formatDateKey(rowDate);
        if (!brandsByDate[dateKey]) {
          brandsByDate[dateKey] = new Set();
        }
        brandsByDate[dateKey].add(brand);

        // Track duplicates: count occurrences of each brand+date combination
        const brandDateKey = `${dateKey}|${brand}`;
        brandDateCounts[brandDateKey] = (brandDateCounts[brandDateKey] || 0) + 1;
      }
    }

    // Find duplicates (brand+date appearing more than once)
    const duplicates = [];
    for (const [key, count] of Object.entries(brandDateCounts)) {
      if (count > 1) {
        const [dateKey, brand] = key.split("|");
        duplicates.push({ date: dateKey, brand: brand, count: count });
      }
    }

    // Validate each date
    const dateResults = [];
    const expectedBrandsSet = new Set(expectedBrands);
    let totalDaysWithMissing = 0;

    // Generate all dates in range
    const allDates = [];
    let currentDate = new Date(startDate);
    while (currentDate <= endDate) {
      allDates.push(formatDateKey(currentDate));
      currentDate.setDate(currentDate.getDate() + 1);
    }

    for (const dateKey of allDates) {
      const brandsOnDate = brandsByDate[dateKey] || new Set();
      const missingOnDate = [];

      for (const expectedBrand of expectedBrands) {
        if (!brandsOnDate.has(expectedBrand)) {
          missingOnDate.push(expectedBrand);
        }
      }

      const hasData = brandsOnDate.size > 0;
      const hasMissing = missingOnDate.length > 0 && hasData;

      if (hasMissing) totalDaysWithMissing++;

      dateResults.push({
        date: dateKey,
        hasData: hasData,
        foundCount: brandsOnDate.size,
        missingCount: hasData ? missingOnDate.length : 0,
        missingBrands: hasData ? missingOnDate : []
      });
    }

    return {
      success: true,
      category: category,
      dateRange: dateRange,
      expectedCount: expectedBrands.length,
      totalDates: allDates.length,
      datesWithMissing: totalDaysWithMissing,
      dateResults: dateResults,
      duplicates: duplicates,
      duplicateCount: duplicates.length
    };

  } catch (e) {
    Logger.log(`Error in BA Dash validation: ${e.message}`);
    return { success: false, error: e.message };
  }
}


/**
 * Formats a date as "DD/MM/YYYY" for display
 */
function formatDateKey(date) {
  const day = String(date.getDate()).padStart(2, '0');
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const year = date.getFullYear();
  return `${day}/${month}/${year}`;
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
 * Gets ALL unique brands from ALL marketplaces (SHO, LAZ, TIK, TOK) in the Brand Master sheet.
 * Used for BSL validation which contains data from all marketplaces.
 *
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet - The Brand Master sheet
 * @returns {string[]} Array of unique brand codes from all marketplaces
 */
function getBrandMasterListAllMarketplaces(sheet) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 10) return []; // No data yet (header is at row 9)

  const validMarketplaces = ["SHO", "LAZ", "TIK", "TOK"];

  // Get both columns: A (Marketplace Code) and B (Brand Code)
  const data = sheet.getRange(10, 1, lastRow - 9, 2).getValues();
  const brandsSet = new Set();

  for (const row of data) {
    const rowMarketplace = (row[0] || "").toString().trim().toUpperCase();
    const brandCode = (row[1] || "").toString().trim();

    // Include brands from all valid marketplaces, skip placeholder/example rows
    if (validMarketplaces.includes(rowMarketplace) &&
        brandCode !== "" &&
        !brandCode.startsWith("(")) {
      brandsSet.add(brandCode.toUpperCase()); // Normalize to uppercase for comparison
    }
  }

  const brands = Array.from(brandsSet);
  Logger.log(`Found ${brands.length} unique brands across all marketplaces`);
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
