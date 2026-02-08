# Restaurant Daily Operations - CSV to Google Sheets Automation

This project moves sales and labor data from CSV files into a Google Sheet. It is built for restaurant daily ops: instead of copying data by hand, you run the script and it uploads the right data into the right tabs.

---

## What It Does

The script reads CSV files (from your computer or from Google Drive) and appends them into specific tabs in a Google Sheet. It handles:

- **Sales data** – payments, revenue, sales by category, daypart summaries
- **Labor data** – payroll export files

It can work in two ways:

- **Testing mode** – uses a local Excel file so you can test without touching Google Sheets
- **Production mode** – writes directly to your Google Sheet

---

## Project Structure

```
BNL-4/
├── csv_to_sheets.py      # Main script
├── config.json           # Your settings (sheet IDs, folders, etc.)
├── config.json.template  # Example config
├── secrets.json.template # Example secrets (you create secrets.json from this)
├── requirements.txt      # Python packages needed
└── setup.py              # Setup helper
```

Files you create yourself:

- `secrets.json` – API credentials and Google Sheet IDs (copy from secrets.json.template, fill in your values, and never commit this file)
- `Sales_Input/` – local folder for sales CSV files (used in testing mode)
- `Labor_Input/` – local folder for labor CSV files (used in testing mode)

In production mode, the script pulls CSV files from Google Drive folders you configure in secrets.json.

---

## How It Runs

1. You run the script (from a terminal or by double‑clicking).
2. A menu appears and asks what to do.
3. You choose an option (for example: “Sales + Latest + Production”).
4. The script validates your setup, finds the right CSVs, and appends them to the sheet.
5. When it finishes, it tells you what was done.

The script reads your Google Sheet to see which dates already exist. It only adds new dates, unless you choose to overwrite.

---

## Architecture (Simple View)

```
Input sources                    Script                      Output
----------------                 ------                      ------
Google Drive folders   -->      csv_to_sheets.py    -->     Google Sheets
(or local folders)              - Finds missing dates
                                - Downloads CSVs (if from Drive)
                                - Maps columns
                                - Appends rows
```

**Testing mode:** Uses a local Excel file. CSV files come from folders like `Sales_Input` and `Labor_Input` on your computer.

**Production mode:** Uses a Google Sheet. CSV files come from Google Drive folders (configured in secrets.json). The script downloads only what it needs, then appends to the sheet.

---

## What You Need To Do

### 1. Install Python and packages

You need Python 3.8 or newer. Then install the required packages:

```
pip install -r requirements.txt
```

### 2. Set up config.json

Copy `config.json.template` to `config.json` if you do not have one yet. Edit:

- `test_mode` – `true` for Excel testing, `false` for Google Sheets
- `excel_file` – name of your local Excel file (used in testing)
- `csv_folder` – name of the folder where sales CSVs are (e.g. `Sales_Input`)

### 3. Set up secrets.json (for production)

Copy `secrets.json.template` to `secrets.json`. Fill in:

- Google Sheet IDs (or the primary/secondary structure if you use multiple sheets)
- OAuth credentials from Google Cloud Console
- Drive folder IDs where your sales and labor CSVs live

Do not share or commit secrets.json.

### 4. Run the script

From a terminal, in the project folder:

```
python csv_to_sheets.py
```

Or run it with `python csv_to_sheets.py help` to see the full menu.

---

## Menu Options and What They Mean

When you run the script, you see a menu. Each option has three parts: **what** to process, **which dates**, and **where** to write.

### What to process

- **Sales** – only sales CSVs (payments, revenue, category, daypart)
- **Labor** – only labor (PayrollExport) CSVs
- **All** – both sales and labor

### Which dates

- **Latest** – process the most recent date, or all missing dates if there are gaps
- **Oldest** – process the oldest missing date first, then continue through all missing dates

### Where to write (mode)

- **Testing** – write to a local Excel file
- **Production** – write to your Google Sheet

### Full menu

| Option | Meaning |
|--------|---------|
| 1 | Sales, latest date, testing (Excel) |
| 2 | Sales, oldest missing first, testing |
| 3 | Sales, latest date, production (Google Sheets) |
| 4 | Sales, oldest missing first, production |
| 5 | Labor, latest date, testing |
| 6 | Labor, oldest missing first, testing |
| 7 | Labor, latest date, production |
| 8 | Labor, oldest missing first, production |
| 9 | All (sales + labor), latest, testing |
| 10 | All, oldest missing first, testing |
| 11 | All, latest, production |
| 12 | All, oldest missing first, production |

**Typical uses:**

- First run with real data: option 11 (All + Latest + Production)
- Catch up on old missing dates: option 12 (All + Oldest + Production)
- Test changes safely: option 9 (All + Latest + Testing)

---

## Command-Line Flags

You can skip the menu and run with flags:

```
python csv_to_sheets.py --sales --latest --prod
python csv_to_sheets.py --labor --oldest --testing
python csv_to_sheets.py --all --latest --prod
```

| Flag | Meaning |
|------|---------|
| `--sales` | Process sales only |
| `--labor` | Process labor only |
| `--all` | Process both |
| `--latest` | Use latest date logic |
| `--oldest` | Use oldest missing logic |
| `--testing` | Write to Excel |
| `--prod` | Write to Google Sheets |
| `--dry-run` | Validate only, no writes |
| `--config FILE` | Use a different config file |

If you do not pass any process/date/mode flags, the script shows the interactive menu.

---

## CSV and Tab Mapping

**Sales CSV files (expected names):**

- `Payments summary.csv` → Sales_Payments tab
- `Revenue summary.csv` → Sales_Revenue tab
- `Sales category summary.csv` → Sales_Category tab
- `Service Daypart summary.csv` → Sales_Daypart tab

**Labor:**

- `PayrollExport_*.csv` → Labor_Input tab

Folder names for sales should follow: `SalesSummary_YYYY-MM-DD_YYYY-MM-DD` (the second date is the one used).

---

## Config and Secrets

**config.json** – general settings:

- `google_sheet` – sheet IDs and date column (often overridden by secrets.json)
- `test_mode` – true/false
- `excel_file` – Excel file name for testing
- `csv_folder` – sales CSV folder name
- `overwrite_behavior` – what to do when a date already exists (e.g. ask)

**secrets.json** (create from secrets.json.template):

- `google_sheet_ids` – primary and optional secondary sheet IDs
- `active_sheet` – which sheet to use
- `oauth_credentials` – from Google Cloud Console
- `drive_inputs` – Drive folder IDs for sales and labor CSVs

Secrets.json is for sensitive data and is not committed to the repository.

---

## Common Issues

**"Rate limit hit"** – Google is limiting requests. The script retries with delays. If it happens often, wait longer between runs or reduce how much you process at once.

**"CSV file is empty"** – The CSV has no usable data. Check that the file is not corrupted and that it has a header row and at least one data row.

**"Could not find oldest missing week"** – All dates from your input folders are already in the sheet, or the script could not find any matching folders.

**"No columns to parse from file"** – Same as empty CSV: the file has no valid content. The script will skip it and continue.

**OAuth / authentication errors** – Check that secrets.json has valid OAuth credentials and that you have completed the Google sign‑in flow. You may need to delete token.pickle and sign in again.

---

## Requirements

- Python 3.8+
- pandas, openpyxl, gspread, google-auth, google-auth-oauthlib, google-auth-httplib2, google-api-python-client (see requirements.txt)
