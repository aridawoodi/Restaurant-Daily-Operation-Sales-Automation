function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu("Ops Automation")
    .addItem("Run Latest Week", "runLoadLatest")
    .addItem("Run Oldest Missing Week", "runLoadOldest")
    .addToUi();
}

function runLoad() {
  const context = createRunContext();
  logMessage("INFO", "Run started.");

  try {
    processSalesInput(context, CONFIG.processOldest);
    processLaborInput(context, CONFIG.processOldest);
    logMessage("INFO", "Run completed.");
  } catch (error) {
    logMessage("ERROR", "Run failed: " + error.message);
    throw error;
  }
}

function runLoadFromButton() {
  runLoad();
}

function runLoadLatest() {
  runLoadWithOptions({ processOldest: false });
}

function runLoadOldest() {
  runLoadWithOptions({ processOldest: true });
}

function runLoadWithOptions(options) {
  const context = createRunContext();
  const processOldest = options && options.processOldest === true;
  const modeLabel = processOldest ? "oldest missing" : "latest";
  logMessage("INFO", "Run started (" + modeLabel + ").");

  try {
    processSalesInput(context, processOldest);
    processLaborInput(context, processOldest);
    logMessage("INFO", "Run completed (" + modeLabel + ").");
  } catch (error) {
    logMessage("ERROR", "Run failed (" + modeLabel + "): " + error.message);
    throw error;
  }
}
