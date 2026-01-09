# Quick Start Guide

Get up and running with the CSV to Google Sheets automation in 5 steps.

## Step 1: Install Python Dependencies

```bash
pip install -r requirements.txt
```

## Step 2: Run Setup Script

```bash
python setup.py
```

This will:
- Check if dependencies are installed
- Create the CSV folder (default: `daily_data/`)
- Create `config.json` from template
- Validate your setup

## Step 3: Set Up Google Sheets API

Follow `setup_instructions.md` to:
1. Create a Google Cloud Project
2. Enable Google Sheets API
3. Create service account credentials
4. Download `credentials.json`
5. Share your Google Sheet with the service account email

## Step 4: Configure the Script

Edit `config.json` and set:
- `google_sheet.sheet_id`: Your Google Sheet ID (from the URL)
- `google_sheet.sheet_name`: Usually "daily ops"
- `google_sheet.date_column`: Name of your date column in the sheet
- `csv_folder`: Folder where you'll download CSV files

## Step 5: Test with Dry Run

```bash
python csv_to_sheets.py --dry-run
```

This validates your configuration and shows what would be uploaded **without actually uploading**.

## Step 6: Download and Process CSV Files

1. Download your CSV files from Toast (or other source)
2. Place them in the `daily_data/` folder (or your configured folder)
3. Run the automation:
   ```bash
   python csv_to_sheets.py
   ```

## Daily Workflow

Once set up:
1. Download yesterday's CSV files
2. Place them in `daily_data/`
3. Run: `python csv_to_sheets.py`
4. Confirm overwrites if prompted
5. Verify data in Google Sheet

## Troubleshooting

**"Credentials file not found"**
→ Follow Step 3 above

**"Google Sheet ID not configured"**
→ Edit `config.json` and set the `sheet_id` field

**"No CSV files found"**
→ Make sure CSV files are in the configured folder

**"No row found for date"**
→ Verify the date exists in your Google Sheet and the `date_column` name matches

For more details, see `README.md`.
