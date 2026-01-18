function createRunContext() {
  const spreadsheet = SpreadsheetApp.openById(CONFIG.spreadsheetId);
  return {
    spreadsheet: spreadsheet,
    timezone: spreadsheet.getSpreadsheetTimeZone()
  };
}

function processSalesInput(context, processOldest) {
  const salesFolder = DriveApp.getFolderById(CONFIG.salesFolderId);
  let targetSalesFolder = null;
  let weekEndingDate = null;

  if (processOldest) {
    const oldestMissing = findOldestMissingSalesFolder(salesFolder, context.spreadsheet, context.timezone);
    if (!oldestMissing) {
      logMessage("INFO", "No missing sales weeks found.");
      return;
    }
    targetSalesFolder = oldestMissing.folder;
    weekEndingDate = oldestMissing.weekEndingDate;
  } else {
    targetSalesFolder = findLatestSalesFolder(salesFolder);
    if (!targetSalesFolder) {
      logMessage("WARN", "No SalesSummary folders found in Sales input folder.");
      return;
    }
    weekEndingDate = extractWeekEndingDateFromSalesFolderName(targetSalesFolder.getName());
  }

  if (!weekEndingDate) {
    logMessage("ERROR", "Could not parse week ending date from folder: " + targetSalesFolder.getName());
    return;
  }

  const filesByName = listFilesByName(targetSalesFolder);
  const tabMapping = CONFIG.salesCsvToTab;
  const tabNames = Object.keys(tabMapping);

  for (let i = 0; i < tabNames.length; i++) {
    const csvName = tabNames[i];
    const tabName = tabMapping[csvName];
    const file = filesByName[csvName];

    if (!file) {
      logMessage("WARN", "Missing sales CSV: " + csvName + " in " + targetSalesFolder.getName());
      continue;
    }

    const sheet = context.spreadsheet.getSheetByName(tabName);
    if (!sheet) {
      logMessage("ERROR", "Sales tab not found: " + tabName);
      continue;
    }

    if (checkWeekEndingExists(sheet, weekEndingDate, context.timezone)) {
      const shouldOverride = confirmOverride(tabName, weekEndingDate, context.timezone);
      if (!shouldOverride) {
        logMessage("INFO", "Skipped sales tab (user chose not to override): " + tabName);
        continue;
      }
      deleteRowsWithWeekEnding(sheet, weekEndingDate, context.timezone);
    }

    const appended = appendCsvFileToSheet(file, sheet, weekEndingDate, context.timezone);
    if (appended > 0) {
      logMessage("INFO", "Appended " + appended + " row(s) to " + tabName);
    }
  }
}

function processLaborInput(context, processOldest) {
  const laborFolder = DriveApp.getFolderById(CONFIG.laborFolderId);
  const payrollFiles = listPayrollFiles(laborFolder);

  if (payrollFiles.length === 0) {
    logMessage("WARN", "No PayrollExport CSV files found in Labor input folder.");
    return;
  }

  let fileToProcess = null;
  let targetWeekEnding = null;
  if (processOldest) {
    const oldestMissing = findOldestMissingPayrollFile(payrollFiles, context.spreadsheet, context.timezone);
    if (!oldestMissing) {
      logMessage("INFO", "No missing labor weeks found.");
      return;
    }
    fileToProcess = oldestMissing.file;
    targetWeekEnding = oldestMissing.weekEndingDate;
  } else {
    payrollFiles.sort((a, b) => b.getLastUpdated().getTime() - a.getLastUpdated().getTime());
    const latestFile = payrollFiles[0];
    const latestWeekEnding = extractWeekEndingDateFromPayrollFile(latestFile.getName());

    if (!latestWeekEnding) {
      logMessage("ERROR", "Could not parse week ending date from " + latestFile.getName());
      return;
    }

    const duplicateFiles = payrollFiles.filter(function (file) {
      const fileWeek = extractWeekEndingDateFromPayrollFile(file.getName());
      return fileWeek && normalizeDateString(fileWeek, context.timezone) === normalizeDateString(latestWeekEnding, context.timezone);
    });

    fileToProcess = latestFile;
    targetWeekEnding = latestWeekEnding;
    if (duplicateFiles.length > 1) {
      const selected = promptForPayrollFileChoice(duplicateFiles, latestWeekEnding, context.timezone);
      if (!selected) {
        logMessage("INFO", "Skipped labor input (user chose to skip).");
        return;
      }
      fileToProcess = selected;
    }
  }

  const laborSheet = context.spreadsheet.getSheetByName(CONFIG.laborTabName);
  if (!laborSheet) {
    logMessage("ERROR", "Labor tab not found: " + CONFIG.laborTabName);
    return;
  }

  if (checkWeekEndingExists(laborSheet, targetWeekEnding, context.timezone)) {
    const shouldOverride = confirmOverride(CONFIG.laborTabName, targetWeekEnding, context.timezone);
    if (!shouldOverride) {
      logMessage("INFO", "Skipped labor tab (user chose not to override).");
      return;
    }
    deleteRowsWithWeekEnding(laborSheet, targetWeekEnding, context.timezone);
  }

  const startRow = laborSheet.getLastRow() + 1;
  const appended = appendCsvFileToSheet(fileToProcess, laborSheet, targetWeekEnding, context.timezone);
  if (appended > 0) {
    addClassificationFormulas(laborSheet, startRow, appended);
    logMessage("INFO", "Appended " + appended + " row(s) to " + CONFIG.laborTabName);
  }
}
