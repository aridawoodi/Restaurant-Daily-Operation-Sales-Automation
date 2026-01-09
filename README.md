# CSV to Google Sheets Automation

Automation script to upload daily restaurant sales and inventory CSV files to a Google Sheet. Designed for restaurant operations with Toast POS system exports.

## Overview

This automation:
- Reads CSV files from a configurable subfolder
- Extracts dates from CSV file content
- Maps CSV columns to your Google Sheet "daily ops" structure
- Uploads data to Google Sheets with overwrite confirmation
- Handles multiple CSV file types (Sales, Payments, Tips, Tax, etc.)

## Prerequisites

- Python 3.8 or higher
- Google Cloud Project with Sheets API enabled
- Google Sheet with "daily ops" sheet
- Service account credentials (see `setup_instructions.md`)

## Installation

### 1. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 2. Set Up Google Sheets API

Follow the detailed instructions in `setup_instructions.md` to:
- Create a Google Cloud Project
- Enable Google Sheets API
- Create service account credentials
- Share your Google Sheet with the service account

### 3. Configure the Script

1. Copy `config.json.template` to `config.json`:
   ```bash
   copy config.json.template config.json
   ```

2. Edit `config.json` and set:
   - `google_sheet.sheet_id`: Your Google Sheet ID (from the URL)
   - `google_sheet.sheet_name`: Usually "daily ops"
   - `google_sheet.date_column`: Name of your date column in the sheet
   - `csv_folder`: Folder name where you'll download CSV files (default: "daily_data")
   - `credentials_file`: Path to your credentials.json (default: "credentials.json")

3. Customize column mappings in `config.json` to match your Google Sheet structure

## Usage

### Step 1: Analyze Your Template (First Time Only)

Before running the automation, analyze your XLSX template to understand its structure:

```bash
python analyze_structure.py
```

This will:
- Read your XLSX template file
- Analyze all CSV files in the current directory
- Generate a `structure_analysis.json` with suggested mappings
- Help you understand the column structure

### Step 2: Download CSV Files

1. Download your daily CSV files from Toast (or other sources)
2. Place them in the configured CSV folder (default: `daily_data/`)
3. Ensure CSV files contain date information (in filename or content)

### Step 3: Test Configuration (Dry Run)

Before uploading data, test your configuration:

```bash
python csv_to_sheets.py --dry-run
```

This will:
- Validate your configuration
- Show which CSV files would be processed
- Display what data would be uploaded
- **Not actually upload anything** to Google Sheets

### Step 4: Run the Automation

```bash
python csv_to_sheets.py
```

The script will:
- Find all CSV files in the configured folder
- Extract dates from each CSV file
- Match dates to rows in your Google Sheet
- Ask for confirmation if data already exists (if `overwrite_behavior: "ask"`)
- Upload mapped data to the appropriate row

### Command Line Options

- `--dry-run`: Test configuration and preview data without uploading
- `--config <file>`: Specify a different config file (default: `config.json`)

## Configuration

### CSV Folder Structure

```
Sales/
├── csv_to_sheets.py
├── config.json
├── credentials.json
├── daily_data/          # CSV files go here
│   ├── Sales category summary.csv
│   ├── Payments summary.csv
│   ├── Tip summary.csv
│   └── ...
└── ...
```

### Column Mapping

The `config.json` file contains mappings for each CSV file type. Each mapping specifies:

- **column_mappings**: Direct column-to-column mappings
- **rules**: How to extract values (first, total, sum, filter, category)
- **special_mappings**: Complex mappings like category breakdowns

Example mapping for Sales category summary:
```json
"Sales category summary.csv": {
  "column_mappings": {
    "Net sales": "Net Sales",
    "Gross sales": "Gross Sales"
  },
  "special_mappings": {
    "category_breakdown": {
      "type": "category_breakdown",
      "category_column": "Sales category",
      "value_column": "Net sales",
      "target_columns": {
        "Food": "Food Sales",
        "Liquor": "Liquor Sales",
        "Beer": "Beer Sales"
      }
    }
  }
}
```

### Overwrite Behavior

Set `overwrite_behavior` in `config.json`:
- `"ask"`: Prompt user when data exists (default)
- `"overwrite"`: Always overwrite existing data
- `"skip"`: Skip if data exists
- `"append"`: Append as new row (not recommended for daily data)

## Date Extraction

The script extracts dates from CSV files using multiple strategies:

1. **Date column in CSV**: Looks for columns named "date", "yyyyMMdd", or "day"
2. **Filename pattern**: Extracts dates from filenames (e.g., `Sales_2026-01-06.csv`)
3. **Multiple formats supported**:
   - `2026-01-06` (YYYY-MM-DD)
   - `01/06/2026` (MM/DD/YYYY)
   - `20260106` (YYYYMMDD)

Dates are matched to rows in your Google Sheet based on the configured `date_column`.

## CSV Files Supported

The script handles these CSV file types:

- **Sales category summary.csv**: Food/Liquor/Beer breakdown
- **Net sales summary.csv**: Overall net sales
- **Payments summary.csv**: Payment type breakdowns
- **Revenue summary.csv**: Revenue components
- **Tip summary.csv**: Tips collected/refunded
- **Tax summary.csv**: Tax breakdowns
- **Cash summary.csv**: Cash reconciliation
- **Sales by day.csv**: Daily sales totals
- **Service Daypart summary.csv**: Service period data
- And more...

Add new mappings in `config.json` as needed.

## Troubleshooting

### "No CSV files found"
- Verify CSV files are in the configured `csv_folder`
- Check that files have `.csv` extension
- Ensure the folder path in `config.json` is correct

### "Could not extract date"
- CSV file should contain a date column or date in filename
- Check that dates are in a recognizable format
- See Date Extraction section above for supported formats

### "No row found for date"
- Verify the date exists in your Google Sheet
- Check that `date_column` in `config.json` matches your sheet
- Ensure date formats are compatible

### "Column not found in sheet headers"
- Verify column names in `config.json` match your Google Sheet exactly
- Check for typos and case sensitivity
- Column names must match exactly (including spaces)

### Authentication Errors
- Verify `credentials.json` is in the correct location
- Ensure Google Sheet is shared with the service account email
- Check `setup_instructions.md` for detailed auth setup

## Workflow Example

Daily workflow:
1. **Morning**: Download yesterday's CSV files from Toast
2. **Place files**: Move CSV files to `daily_data/` folder
3. **Run script**: Execute `python csv_to_sheets.py`
4. **Confirm**: Review and confirm overwrites if prompted
5. **Verify**: Check Google Sheet to verify data uploaded correctly

## Customization

### Adding New CSV File Types

1. Analyze the CSV structure
2. Add mapping entry to `config.json`:
   ```json
   "Your CSV file.csv": {
     "column_mappings": {
       "CSV Column": "Sheet Column"
     },
     "rules": {
       "CSV Column": {
         "type": "first"  // or "total", "sum", "category", etc.
       }
     }
   }
   ```

### Modifying Date Extraction

Edit the `extract_date_from_csv()` method in `csv_to_sheets.py` to add custom date extraction logic.

### Custom Value Extraction Rules

Supported rule types:
- `"first"`: Use first value (default)
- `"total"`: Find row with "Total" and use that value
- `"sum"`: Sum all values in column
- `"filter"`: Filter by specific value in another column
- `"category"`: Extract value for specific category

## Files

- `csv_to_sheets.py` - Main automation script
- `analyze_structure.py` - Template analysis tool
- `config.json` - Configuration (create from template)
- `config.json.template` - Configuration template
- `requirements.txt` - Python dependencies
- `setup_instructions.md` - Google Sheets API setup guide
- `README.md` - This file

## Support

For issues or questions:
1. Check the Troubleshooting section above
2. Review `setup_instructions.md` for authentication issues
3. Run `analyze_structure.py` to understand your template structure
4. Verify `config.json` column mappings match your Google Sheet

## License

This automation script is provided as-is for restaurant operations use.
