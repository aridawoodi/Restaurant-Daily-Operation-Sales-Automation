function findLatestSalesFolder(salesFolder) {
  const folders = [];
  const iterator = salesFolder.getFolders();

  while (iterator.hasNext()) {
    const folder = iterator.next();
    if (!folder.getName().startsWith(CONFIG.salesFolderPrefix)) {
      continue;
    }
    if (!CONFIG.salesFolderPattern.test(folder.getName())) {
      continue;
    }
    const weekEnding = extractWeekEndingDateFromSalesFolderName(folder.getName());
    if (weekEnding) {
      folders.push({ folder: folder, weekEnding: weekEnding });
    }
  }

  if (folders.length === 0) {
    return null;
  }

  folders.sort((a, b) => b.weekEnding.getTime() - a.weekEnding.getTime());
  return folders[0].folder;
}

function findOldestMissingSalesFolder(salesFolder, spreadsheet, timezone) {
  const folders = [];
  const iterator = salesFolder.getFolders();

  while (iterator.hasNext()) {
    const folder = iterator.next();
    if (!folder.getName().startsWith(CONFIG.salesFolderPrefix)) {
      continue;
    }
    if (!CONFIG.salesFolderPattern.test(folder.getName())) {
      continue;
    }
    const weekEnding = extractWeekEndingDateFromSalesFolderName(folder.getName());
    if (weekEnding) {
      folders.push({ folder: folder, weekEnding: weekEnding });
    }
  }

  if (folders.length === 0) {
    return null;
  }

  const existingDates = getAllExistingWeekEndingDatesForSales(spreadsheet, timezone);
  folders.sort((a, b) => a.weekEnding.getTime() - b.weekEnding.getTime());

  for (let i = 0; i < folders.length; i++) {
    const dateString = normalizeDateString(folders[i].weekEnding, timezone);
    if (!existingDates[dateString]) {
      return { folder: folders[i].folder, weekEndingDate: folders[i].weekEnding };
    }
  }

  return null;
}

function extractWeekEndingDateFromSalesFolderName(folderName) {
  const match = folderName.match(/SalesSummary_\d{4}-\d{2}-\d{2}_(\d{4})-(\d{2})-(\d{2})/);
  if (!match) {
    return null;
  }
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  return new Date(year, month - 1, day);
}

function listFilesByName(folder) {
  const files = {};
  const iterator = folder.getFiles();
  while (iterator.hasNext()) {
    const file = iterator.next();
    files[file.getName()] = file;
  }
  return files;
}

function listPayrollFiles(folder) {
  const files = [];
  const iterator = folder.getFiles();
  while (iterator.hasNext()) {
    const file = iterator.next();
    if (CONFIG.payrollFilePattern.test(file.getName())) {
      files.push(file);
    }
  }
  return files;
}

function findOldestMissingPayrollFile(payrollFiles, spreadsheet, timezone) {
  const filesWithDates = [];
  payrollFiles.forEach(function (file) {
    const weekEnding = extractWeekEndingDateFromPayrollFile(file.getName());
    if (weekEnding) {
      filesWithDates.push({ file: file, weekEnding: weekEnding });
    }
  });

  if (filesWithDates.length === 0) {
    return null;
  }

  const existingDates = getAllExistingWeekEndingDatesForTab(
    spreadsheet,
    CONFIG.laborTabName,
    timezone
  );

  filesWithDates.sort((a, b) => a.weekEnding.getTime() - b.weekEnding.getTime());
  for (let i = 0; i < filesWithDates.length; i++) {
    const dateString = normalizeDateString(filesWithDates[i].weekEnding, timezone);
    if (!existingDates[dateString]) {
      return { file: filesWithDates[i].file, weekEndingDate: filesWithDates[i].weekEnding };
    }
  }

  return null;
}

function extractWeekEndingDateFromPayrollFile(fileName) {
  const match = fileName.match(/PayrollExport_\d{4}_\d{2}_\d{2}-(\d{4})_(\d{2})_(\d{2})/);
  if (!match) {
    return null;
  }
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  return new Date(year, month - 1, day);
}

function checkWeekEndingExists(sheet, weekEndingDate, timezone) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) {
    return false;
  }
  const values = sheet.getRange(2, 1, lastRow - 1, 1).getValues();
  const target = normalizeDateString(weekEndingDate, timezone);
  for (let i = 0; i < values.length; i++) {
    const value = values[i][0];
    if (!value) {
      continue;
    }
    const normalized = normalizeDateString(value, timezone);
    if (normalized && normalized === target) {
      return true;
    }
  }
  return false;
}

function getAllExistingWeekEndingDatesForSales(spreadsheet, timezone) {
  const existing = {};
  const tabNames = Object.keys(CONFIG.salesCsvToTab).map(function (csvName) {
    return CONFIG.salesCsvToTab[csvName];
  });

  tabNames.forEach(function (tabName) {
    const tabExisting = getAllExistingWeekEndingDatesForTab(spreadsheet, tabName, timezone);
    Object.keys(tabExisting).forEach(function (dateString) {
      existing[dateString] = true;
    });
  });

  return existing;
}

function getAllExistingWeekEndingDatesForTab(spreadsheet, tabName, timezone) {
  const sheet = spreadsheet.getSheetByName(tabName);
  const existing = {};
  if (!sheet) {
    return existing;
  }

  const lastRow = sheet.getLastRow();
  if (lastRow < 2) {
    return existing;
  }

  const values = sheet.getRange(2, 1, lastRow - 1, 1).getValues();
  for (let i = 0; i < values.length; i++) {
    const value = values[i][0];
    if (!value) {
      continue;
    }
    const normalized = normalizeDateString(value, timezone);
    if (normalized) {
      existing[normalized] = true;
    }
  }

  return existing;
}

function deleteRowsWithWeekEnding(sheet, weekEndingDate, timezone) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) {
    return;
  }

  const values = sheet.getRange(2, 1, lastRow - 1, 1).getValues();
  const target = normalizeDateString(weekEndingDate, timezone);
  const rowsToDelete = [];

  for (let i = 0; i < values.length; i++) {
    const value = values[i][0];
    if (!value) {
      continue;
    }
    const normalized = normalizeDateString(value, timezone);
    if (normalized && normalized === target) {
      rowsToDelete.push(i + 2);
    }
  }

  rowsToDelete.sort((a, b) => b - a);
  rowsToDelete.forEach(function (row) {
    sheet.deleteRow(row);
  });
}

function appendCsvFileToSheet(file, sheet, weekEndingDate, timezone) {
  const csvText = file.getBlob().getDataAsString();
  const rows = Utilities.parseCsv(csvText);
  if (!rows || rows.length < 2) {
    logMessage("WARN", "CSV has no data rows: " + file.getName());
    return 0;
  }

  const csvHeaders = rows[0].map(String);
  const sheetHeaders = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0].map(String);
  if (sheetHeaders.length === 0) {
    logMessage("ERROR", "Sheet has no headers: " + sheet.getName());
    return 0;
  }

  const csvHeaderLookup = buildHeaderLookup(csvHeaders);
  const classificationHeaders = CONFIG.classificationHeaderNames;

  const dataRows = rows.slice(1);
  const output = [];
  const weekEndingString = normalizeDateString(weekEndingDate, timezone);

  dataRows.forEach(function (dataRow) {
    const rowValues = new Array(sheetHeaders.length).fill("");
    rowValues[0] = weekEndingString;

    for (let colIndex = 0; colIndex < sheetHeaders.length; colIndex++) {
      if (colIndex === 0) {
        continue;
      }
      const sheetHeader = String(sheetHeaders[colIndex] || "").trim();
      const headerKey = sheetHeader.toLowerCase();
      if (classificationHeaders.indexOf(headerKey) !== -1) {
        rowValues[colIndex] = "";
        continue;
      }

      const csvIndex = csvHeaderLookup[headerKey];
      if (csvIndex === undefined) {
        rowValues[colIndex] = "";
        continue;
      }
      const rawValue = dataRow[csvIndex];
      rowValues[colIndex] = normalizeCellValue(rawValue);
    }
    output.push(rowValues);
  });

  if (output.length === 0) {
    return 0;
  }

  const startRow = sheet.getLastRow() + 1;
  sheet.getRange(startRow, 1, output.length, output[0].length).setValues(output);
  return output.length;
}

function addClassificationFormulas(sheet, startRow, rowsAppended) {
  const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0].map(String);
  const classificationCol = findHeaderIndex(headers, CONFIG.classificationHeaderNames);
  const jobTitleCol = findHeaderIndex(headers, CONFIG.jobTitleHeaderNames);

  if (!classificationCol || !jobTitleCol) {
    return;
  }

  const jobTitleLetter = columnToLetter(jobTitleCol);
  const formulas = [];
  for (let i = 0; i < rowsAppended; i++) {
    const row = startRow + i;
    const formula = '=IFERROR(VLOOKUP(' + jobTitleLetter + row + ', Job_Classification_Lookup!$A$2:$B$100, 2, FALSE), "Other")';
    formulas.push([formula]);
  }

  sheet.getRange(startRow, classificationCol, rowsAppended, 1).setFormulas(formulas);
}

function confirmOverride(tabName, weekEndingDate, timezone) {
  const ui = SpreadsheetApp.getUi();
  const dateString = normalizeDateString(weekEndingDate, timezone);
  const response = ui.alert(
    "Override existing data?",
    "Week ending " + dateString + " already exists in " + tabName + ". Override existing rows?",
    ui.ButtonSet.YES_NO
  );
  return response === ui.Button.YES;
}

function promptForPayrollFileChoice(files, weekEndingDate, timezone) {
  const ui = SpreadsheetApp.getUi();
  const dateString = normalizeDateString(weekEndingDate, timezone);
  let message = "Multiple PayrollExport files found for week ending " + dateString + ":\n\n";
  files.forEach(function (file, index) {
    message += (index + 1) + ". " + file.getName() + "\n";
  });
  message += "\nEnter the number to process, or type SKIP.";

  for (let attempt = 0; attempt < 3; attempt++) {
    const result = ui.prompt("Choose PayrollExport file", message, ui.ButtonSet.OK_CANCEL);
    if (result.getSelectedButton() === ui.Button.CANCEL) {
      return null;
    }
    const text = result.getResponseText().trim();
    if (text.toLowerCase() === "skip") {
      return null;
    }
    const choice = Number(text);
    if (!Number.isNaN(choice) && choice >= 1 && choice <= files.length) {
      return files[choice - 1];
    }
  }

  logMessage("WARN", "Invalid PayrollExport selection. Defaulting to latest file.");
  return files[0];
}

function buildHeaderLookup(headers) {
  const lookup = {};
  headers.forEach(function (header, index) {
    const key = String(header || "").trim().toLowerCase();
    if (key && lookup[key] === undefined) {
      lookup[key] = index;
    }
  });
  return lookup;
}

function findHeaderIndex(headers, acceptable) {
  for (let i = 0; i < headers.length; i++) {
    const header = String(headers[i] || "").trim().toLowerCase();
    if (acceptable.indexOf(header) !== -1) {
      return i + 1;
    }
  }
  return null;
}

function normalizeDateString(value, timezone) {
  if (!value) {
    return null;
  }
  if (Object.prototype.toString.call(value) === "[object Date]" && !isNaN(value)) {
    return Utilities.formatDate(value, timezone, "yyyy-MM-dd");
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
      return trimmed;
    }
    const parsed = new Date(trimmed);
    if (!isNaN(parsed)) {
      return Utilities.formatDate(parsed, timezone, "yyyy-MM-dd");
    }
  }
  return null;
}

function normalizeCellValue(value) {
  if (value === null || value === undefined || value === "") {
    return "";
  }
  const text = String(value).trim();
  if (text === "") {
    return "";
  }
  if (/^-?\d+(\.\d+)?$/.test(text)) {
    return Number(text);
  }
  return text;
}

function columnToLetter(column) {
  let temp = "";
  let letter = "";
  while (column > 0) {
    temp = (column - 1) % 26;
    letter = String.fromCharCode(temp + 65) + letter;
    column = (column - temp - 1) / 26;
  }
  return letter;
}

function logMessage(level, message) {
  const sheet = getOrCreateLogSheet();
  const timestamp = new Date();
  sheet.appendRow([timestamp, level, message]);
}

function getOrCreateLogSheet() {
  const spreadsheet = SpreadsheetApp.openById(CONFIG.spreadsheetId);
  let sheet = spreadsheet.getSheetByName(CONFIG.logSheetName);
  if (!sheet) {
    sheet = spreadsheet.insertSheet(CONFIG.logSheetName);
    sheet.appendRow(["Timestamp", "Level", "Message"]);
  }
  return sheet;
}
