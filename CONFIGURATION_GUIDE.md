# Configuration Guide

This guide explains where to configure your actual Google Sheet ID and email address after cloning the repository.

## Required Configuration

### 1. Create `secrets.json` File

**Important:** Sensitive data (Sheet ID and email) are stored in `secrets.json`, which is **NOT committed** to the repository.

**What to do:**
1. Copy `secrets.json.template` to `secrets.json`:
   ```bash
   copy secrets.json.template secrets.json
   ```
   Or on Linux/Mac:
   ```bash
   cp secrets.json.template secrets.json
   ```

2. Open `secrets.json` and replace the placeholders with your actual values:
   ```json
   {
     "google_sheet_id": "1JgTZ70ZjdpaYqKtXZ4Qmf_8H57wqXjsWrWwGQxWbj2s",
     "email": "your-email@example.com",
     "oauth_credentials": {
       "installed": {
         "client_id": "your-client-id.apps.googleusercontent.com",
         "project_id": "your-project-id",
         "auth_uri": "https://accounts.google.com/o/oauth2/auth",
         "token_uri": "https://oauth2.googleapis.com/token",
         "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
         "client_secret": "your-client-secret",
         "redirect_uris": ["http://localhost"]
       }
      },
      "drive_inputs": {
        "root_id": "YOUR_DRIVE_ROOT_FOLDER_ID",
        "sales_input_id": "YOUR_SALES_INPUT_FOLDER_ID",
        "labor_input_id": "YOUR_LABOR_INPUT_FOLDER_ID"
      }
   }
   ```

**How to find your Google Sheet ID:**
1. Open your Google Sheet in a browser
2. Look at the URL: `https://docs.google.com/spreadsheets/d/SHEET_ID_HERE/edit`
3. Copy the long string between `/d/` and `/edit` - that's your Sheet ID

**Email Address:**
- This is the email address you'll use when setting up OAuth test users in Google Cloud Console
- It's only used in error messages/help text to guide you during setup

---

### 2. How It Works

The script automatically loads values from `secrets.json` if it exists:
- **Google Sheet ID** → Overrides `config.json` `google_sheet.sheet_id` if present
- **Email** → Used in error messages and troubleshooting help text
- **OAuth Credentials** → Used for Google OAuth authentication (replaces `oauth_credentials.json` file)
- **Drive Inputs** → Used in production mode to download Sales/Labor input files from Google Drive

**If `secrets.json` doesn't exist:**
- The script will use placeholder values from `config.json`
- You'll see `YOUR_GOOGLE_SHEET_ID_HERE` and `YOUR_EMAIL_HERE` in messages
- Production mode will fall back to default Drive folder IDs if `drive_inputs` is missing

---

## Quick Setup Checklist

After cloning the repository:

- [ ] Copy `secrets.json.template` to `secrets.json`
- [ ] Edit `secrets.json` and add your actual:
  - Google Sheet ID
  - Email address
  - OAuth credentials (optional - can use `oauth_credentials.json` file instead)
  - Drive folder IDs for Sales/Labor inputs (production mode)
- [ ] Follow `setup_instructions.md` to set up Google Sheets API credentials
- [ ] Create `credentials.json` (for service account) if using service account auth
- [ ] Run the script: `python csv_to_sheets.py`

---

## Security Notes

- ✅ **`secrets.json` is already in `.gitignore`** - it will never be committed
- ✅ **`secrets.json.template` is safe to commit** - it only contains placeholders
- ✅ **Never commit** credential files (`credentials.json`, `oauth_credentials.json`, `token.pickle`)
- ✅ All sensitive data (Sheet ID, email, OAuth credentials) are stored locally in `secrets.json` and never pushed to GitHub
- ✅ You can use `secrets.json` OR separate credential files - both methods work

---

## File Structure

```
project/
├── config.json              # Main configuration (committed)
├── config.json.template     # Template for config.json (committed)
├── secrets.json             # Your actual Sheet ID & email (NOT committed)
├── secrets.json.template    # Template for secrets.json (committed)
├── csv_to_sheets.py         # Main script (committed)
└── .gitignore              # Ensures secrets.json is never committed
```

**What gets committed:**
- ✅ `config.json` (with placeholders)
- ✅ `secrets.json.template` (with placeholders)
- ✅ `csv_to_sheets.py` (reads from secrets.json)

**What does NOT get committed:**
- ❌ `secrets.json` (your actual values)
- ❌ `credentials.json` (OAuth/service account keys)
- ❌ `token.pickle` (OAuth tokens)
