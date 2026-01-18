# Google Sheets API Setup Instructions

Follow these steps to set up Google Sheets API authentication for the CSV automation script.

> **⚠️ Organization Policy Blocking Service Account Keys?**
> 
> If you see an error that service account key creation is disabled by your organization policy (`iam.disableServiceAccountKeyCreation`), you have two options:
> 1. **Use OAuth 2.0** (Option 2 below) - Recommended workaround, no admin approval needed
> 2. **Request policy exception** - Contact your Organization Policy Administrator to disable the constraint

## Option 1: Service Account (Recommended for Automation)

> **Note**: If your organization blocks service account key creation, skip to **Option 2: OAuth 2.0** below.

### Step 1: Create a Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Click on "Select a project" → "New Project"
3. Name your project (e.g., "Restaurant Sales Automation")
4. Click "Create"

### Step 2: Enable Google Sheets API

1. In your project, go to "APIs & Services" → "Library"
2. Search for "Google Sheets API"
3. Click on it and click "Enable"
4. Also search for and enable "Google Drive API"

### Step 3: Create Service Account

1. Go to "APIs & Services" → "Credentials"
2. Click "Create Credentials" → "Service Account"
3. Fill in:
   - **Service account name**: `csv-automation` (or any name)
   - **Service account ID**: Will auto-generate
   - **Description**: "Service account for CSV to Sheets automation"
4. Click "Create and Continue"
5. Skip the optional steps and click "Done"

### Step 4: Create Service Account Key

1. Click on the service account you just created
2. Go to the "Keys" tab
3. Click "Add Key" → "Create new key"
4. Select "JSON" format
5. Click "Create"
6. The JSON file will download automatically

### Step 5: Save Credentials File

1. Rename the downloaded JSON file to `credentials.json`
2. Place it in the same folder as `csv_to_sheets.py`
3. **Important**: Never commit this file to version control (it's already in .gitignore)

### Step 6: Share Google Sheet with Service Account

1. Open your Google Sheet
2. Click the "Share" button (top right)
3. Get the **service account email** from the `credentials.json` file:
   - Open `credentials.json`
   - Find the `client_email` field (looks like: `csv-automation@project-id.iam.gserviceaccount.com`)
4. Paste this email into the "Share" dialog
5. Give it "Editor" access
6. Uncheck "Notify people" (not needed for service accounts)
7. Click "Send"

### Step 7: Get Your Google Sheet ID

1. Open your Google Sheet
2. Look at the URL in your browser
3. The Sheet ID is the long string between `/d/` and `/edit`
   - Example URL: `https://docs.google.com/spreadsheets/d/1abc123xyz456/edit`
   - Sheet ID: `1abc123xyz456`
4. Copy this ID and paste it into `config.json` as `google_sheet.sheet_id`

## Option 2: OAuth 2.0 (Use When Service Account Keys Are Blocked)

OAuth 2.0 is the recommended alternative when your organization blocks service account key creation. After the initial setup, the script will save a token for future use, so you won't need to authenticate every time.

### Step 1: Create a Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Click on "Select a project" → "New Project"
3. Name your project (e.g., "Restaurant Sales Automation")
4. Click "Create"

### Step 2: Enable Google Sheets API

1. In your project, go to "APIs & Services" → "Library"
2. Search for "Google Sheets API"
3. Click on it and click "Enable"
4. Also search for and enable "Google Drive API"

### Step 3: Configure OAuth Consent Screen

1. Go to "APIs & Services" → "OAuth consent screen"
2. Select **"External"** (unless you have a Google Workspace account, then you can use "Internal")
3. Click "Create"
4. Fill in the required information:
   - **App name**: `CSV to Sheets Automation` (or any name)
   - **User support email**: Your email address
   - **Developer contact information**: Your email address
5. Click "Save and Continue"
6. On the "Scopes" page, click "Add or Remove Scopes"
   - Search for and add:
     - `https://www.googleapis.com/auth/spreadsheets`
   - **Note**: Only `spreadsheets` scope is needed for accessing existing Google Sheets
   - Click "Update" then "Save and Continue"
7. On the "Test users" page (if External):
   - Click "Add Users"
   - Add your own email address
   - Click "Add"
   - Click "Save and Continue"
8. Review and click "Back to Dashboard"

### Step 4: Create OAuth Client ID

1. Go to "APIs & Services" → "Credentials"
2. Click "Create Credentials" → "OAuth client ID"
3. Select **"Desktop app"** as the application type
4. Give it a name (e.g., "CSV Automation Desktop Client")
5. Click "Create"
6. A dialog will appear with your Client ID and Client Secret
7. Click "Download JSON" to download the credentials file
8. Close the dialog

### Step 5: Save OAuth Credentials File

1. Rename the downloaded JSON file to `oauth_credentials.json`
2. Place it in the same folder as `csv_to_sheets.py`
3. **Important**: Never commit this file to version control (it's already in .gitignore)

### Step 6: Update Configuration

1. Open `config.json`
2. Add or update the authentication settings:
   ```json
   {
     "auth_method": "oauth",
     "oauth_credentials_file": "oauth_credentials.json",
     "oauth_token_file": "token.pickle",
     ...
   }
   ```
3. Save the file

### Step 7: First-Time Authentication

1. Run the script:
   ```bash
   python csv_to_sheets.py
   ```
2. Your default web browser will open automatically
3. Sign in with your Google account (the one you added as a test user)
4. Review the permissions and click "Continue"
5. If you see "Google hasn't verified this app", click "Advanced" → "Go to CSV to Sheets Automation (unsafe)"
6. Click "Allow" to grant permissions
7. The script will save a token file (`token.pickle`) for future use
8. You won't need to authenticate again unless the token expires

### Step 8: Get Your Google Sheet ID

1. Open your Google Sheet
2. Look at the URL in your browser
3. The Sheet ID is the long string between `/d/` and `/edit`
   - Example URL: `https://docs.google.com/spreadsheets/d/1abc123xyz456/edit`
   - Sheet ID: `1abc123xyz456`
4. Copy this ID and paste it into `config.json` as `google_sheet.sheet_id`

**Note**: With OAuth, you authenticate as yourself, so you automatically have access to any Google Sheets you own or have been shared with. No need to share the sheet with a service account email.

## Troubleshooting

### "Permission denied" error
- Make sure you've shared the Google Sheet with the service account email
- Verify the service account email is correct in the share settings
- Check that the service account has "Editor" permissions

### "File not found" error
- Verify the Sheet ID in `config.json` is correct
- Check that the service account (or OAuth account) has access to the sheet
- Verify the sheet is shared with the correct account

### "Invalid credentials" error
- **For Service Account**: Verify `credentials.json` is in the correct location and valid
- **For OAuth**: Verify `oauth_credentials.json` exists and is valid
- Check that the JSON file is not corrupted
- For OAuth, try deleting `token.pickle` and re-authenticating

### "Invalid scope" or "Scope has changed" error (OAuth)
- This error occurs when the OAuth consent screen scopes don't match what the script requests
- **Solution**: 
  1. Go to Google Cloud Console → "APIs & Services" → "OAuth consent screen"
  2. Click "Edit App"
  3. Go to "Scopes" and ensure only `https://www.googleapis.com/auth/spreadsheets` is added
  4. Remove any `drive.file` scope if present
  5. Save and continue through all steps
  6. Delete `token.pickle` file if it exists
  7. Run the script again to re-authenticate

### Date not found in sheet
- Verify the date column name in `config.json` matches your sheet
- Check that dates in your sheet are formatted consistently
- The script looks for dates in various formats, but exact column name match is needed

## Security Notes

- **Never share your credential files** - they provide access to your Google Sheets
  - Service Account: `credentials.json`
  - OAuth: `oauth_credentials.json` and `token.pickle`
- Keep credential files in the same folder as the script
- All credential files are already in `.gitignore` to prevent accidental commits
- If credentials are compromised:
  - **Service Account**: Delete the service account and create a new one
  - **OAuth**: Revoke access in [Google Account Security](https://myaccount.google.com/permissions) and delete the OAuth client
