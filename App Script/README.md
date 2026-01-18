Apps Script setup

1) Open your Google Sheet and go to Extensions > Apps Script.
2) Create the following files and paste the contents from this folder:
   - Config.gs
   - Main.gs
   - Automation.gs
   - Helpers.gs
3) In Apps Script, set project timezone to match the spreadsheet timezone.
4) Save, then run runLoad once to grant permissions.

Run options
- Use the menu: Ops Automation > Run Latest Week or Run Oldest Missing Week
- Or assign a drawing/button to the function runLoadFromButton (uses CONFIG.processOldest)

Notes
- The script uses DriveApp + SpreadsheetApp and does not require the Drive API for CSV files.
- If a file is an .xlsx instead of .csv, you will need Advanced Drive Service enabled and conversion logic added.
