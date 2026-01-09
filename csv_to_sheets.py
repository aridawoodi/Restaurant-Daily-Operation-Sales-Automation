"""
CSV to Google Sheets Automation Script
Reads daily CSV files from a subfolder and uploads them to Google Sheets 'daily ops' sheet.
"""

import pandas as pd
import os
import json
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import gspread
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from google.oauth2.credentials import Credentials as OAuthCredentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.auth.exceptions import GoogleAuthError
import pickle
import sys

# Fix Windows encoding issues - use ASCII-safe characters
CHECKMARK = "[OK]" if sys.platform == 'win32' else "{CHECKMARK}"
WARNING = "[!]" if sys.platform == 'win32' else "⚠"
CROSS = "[X]" if sys.platform == 'win32' else "{CROSS}"

class CSVToSheetsAutomation:
    def __init__(self, config_path: str = "config.json", dry_run: bool = False):
        """Initialize the automation with configuration file."""
        self.config = self.load_config(config_path)
        self.dry_run = dry_run
        self.gc = None
        self.sheet = None
        self.worksheet = None
        
    def load_config(self, config_path: str) -> Dict:
        """Load configuration from JSON file."""
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file {config_path} not found!")
        
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Validate required config fields
        required_fields = ['google_sheet', 'csv_folder']
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required configuration field: {field}")
        
        return config
    
    def authenticate_google_sheets(self) -> bool:
        """Authenticate with Google Sheets API using service account or OAuth."""
        auth_method = self.config.get('auth_method', 'service_account').lower()
        
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.readonly"  # Needed to access sheets
        ]
        
        try:
            if auth_method == 'oauth':
                creds = self._authenticate_oauth(scope)
            else:
                creds = self._authenticate_service_account(scope)
            
            if not creds:
                return False
            
            self.gc = gspread.authorize(creds)
            
            # Show which account is being used
            try:
                # Try to get user info from the credentials
                if hasattr(creds, 'id_token'):
                    # For OAuth, we can't easily get email from token, but we can try to list sheets
                    print(f"{CHECKMARK} Authenticated successfully")
                else:
                    print(f"{CHECKMARK} Authenticated successfully")
            except:
                pass
            
            # Open the Google Sheet
            sheet_id = self.config['google_sheet']['sheet_id']
            sheet_name = self.config['google_sheet'].get('sheet_name', 'daily ops')
            
            try:
                # Try opening by key first
                self.sheet = self.gc.open_by_key(sheet_id)
                print(f"{CHECKMARK} Successfully opened Google Sheet: {self.sheet.title}")
            except Exception as e:
                error_msg = str(e)
                print(f"\nError opening Google Sheet with ID {sheet_id[:20]}...")
                print(f"Error details: {error_msg}")
                
                # Check if this is the "not supported" error
                if "not supported" in error_msg.lower() or "400" in error_msg:
                    print(f"\n⚠ This error usually means:")
                    print(f"  1. OAuth app is in 'Testing' mode and your account isn't added as a test user")
                    print(f"  2. The OAuth app needs verification (for external users)")
                    print(f"  3. Domain restrictions on the Google Workspace account")
                    print(f"\nTrying alternative methods...")
                    
                    # Method 1: Try opening by URL
                    try:
                        sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
                        self.sheet = self.gc.open_by_url(sheet_url)
                        print(f"{CHECKMARK} Successfully opened Google Sheet via URL: {self.sheet.title}")
                    except Exception as e2:
                        print(f"  Method 1 (URL) failed: {e2}")
                        
                        # Method 2: Try listing all sheets and finding by ID
                        try:
                            print(f"  Trying Method 2: Listing accessible sheets...")
                            all_sheets = self.gc.openall()
                            print(f"  Found {len(all_sheets)} accessible sheet(s)")
                            
                            # Show first few sheets for debugging
                            print(f"  Sample of accessible sheets:")
                            for i, sheet in enumerate(all_sheets[:5], 1):
                                print(f"    {i}. {sheet.title} (ID: {sheet.id[:20]}...)")
                            
                            # Try to find by ID
                            found = False
                            for sheet in all_sheets:
                                if sheet.id == sheet_id:
                                    self.sheet = sheet
                                    print(f"{CHECKMARK} Found sheet by listing: {self.sheet.title}")
                                    found = True
                                    break
                            
                            if not found:
                                # Also try partial match (in case of formatting issues)
                                for sheet in all_sheets:
                                    if sheet.id.replace('-', '') == sheet_id.replace('-', ''):
                                        self.sheet = sheet
                                        print(f"{CHECKMARK} Found sheet by partial ID match: {self.sheet.title}")
                                        found = True
                                        break
                                
                                if not found:
                                    raise Exception(f"Sheet with ID {sheet_id} not found in {len(all_sheets)} accessible sheets")
                        except Exception as e3:
                            print(f"  Method 2 (listing) failed: {e3}")
                            print(f"\n💡 SOLUTION: Add your account as a test user in Google Cloud Console:")
                            print(f"  1. Go to: https://console.cloud.google.com/apis/credentials/consent")
                            print(f"  2. Click 'Edit App'")
                            print(f"  3. Go to 'Test users' section")
                            print(f"  4. Click 'Add Users' and add: info@brugadabar.com")
                            print(f"  5. Save and re-run the script")
                
                if not hasattr(self, 'sheet') or not self.sheet:
                    print(f"\nTroubleshooting:")
                    print(f"  1. Make sure you authorized with the CORRECT Google account")
                    print(f"     - Use the account that OWNS or has EDIT access to the sheet")
                    print(f"     - Based on your sheet, use: info@brugadabar.com")
                    print(f"  2. Verify the Sheet ID is correct: {sheet_id}")
                    print(f"  3. Check that the sheet is shared with the authorized account")
                    print(f"  4. Try opening the sheet in your browser to verify access")
                    print(f"  5. Delete token.pickle and re-run to re-authenticate with correct account")
                    return False
            
            # Try to get the worksheet
            try:
                self.worksheet = self.sheet.worksheet(sheet_name)
                print(f"{CHECKMARK} Using worksheet: '{sheet_name}'")
            except gspread.exceptions.WorksheetNotFound:
                print(f"\n⚠ Worksheet '{sheet_name}' not found!")
                print(f"Available worksheets: {[ws.title for ws in self.sheet.worksheets()]}")
                print(f"\nTrying to use the first worksheet...")
                self.worksheet = self.sheet.sheet1
                print(f"{CHECKMARK} Using worksheet: '{self.worksheet.title}'")
            except Exception as e:
                print(f"\nError accessing worksheet '{sheet_name}': {e}")
                print(f"Available worksheets: {[ws.title for ws in self.sheet.worksheets()]}")
                return False
            
            return True
            
        except GoogleAuthError as e:
            print(f"\nAuthentication error: {e}")
            return False
        except Exception as e:
            print(f"\nError connecting to Google Sheets: {e}")
            return False
    
    def _authenticate_service_account(self, scope: List[str]):
        """Authenticate using service account credentials."""
        credentials_path = self.config.get('credentials_file', 'credentials.json')
        
        if not os.path.exists(credentials_path):
            print(f"\nError: Credentials file '{credentials_path}' not found!")
            print("Please follow the setup instructions in setup_instructions.md")
            return None
        
        try:
            creds = ServiceAccountCredentials.from_service_account_file(credentials_path, scopes=scope)
            return creds
        except Exception as e:
            print(f"\nService account authentication error: {e}")
            return None
    
    def _authenticate_oauth(self, scope: List[str]):
        """Authenticate using OAuth 2.0 flow."""
        credentials_path = self.config.get('oauth_credentials_file', 'oauth_credentials.json')
        token_path = self.config.get('oauth_token_file', 'token.pickle')
        
        if not os.path.exists(credentials_path):
            print(f"\nError: OAuth credentials file '{credentials_path}' not found!")
            print("Please follow the OAuth setup instructions in setup_instructions.md")
            return None
        
        creds = None
        
        # Load existing token if available
        if os.path.exists(token_path):
            try:
                with open(token_path, 'rb') as token:
                    creds = pickle.load(token)
            except Exception as e:
                print(f"Warning: Could not load existing token: {e}")
        
        # If there are no (valid) credentials available, let the user log in
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                # Refresh the token
                try:
                    creds.refresh(Request())
                except Exception as e:
                    print(f"Error refreshing token: {e}")
                    creds = None
            
            if not creds:
                # Run the OAuth flow
                try:
                    flow = InstalledAppFlow.from_client_secrets_file(credentials_path, scope)
                    creds = flow.run_local_server(port=0)
                except Exception as e:
                    print(f"\nOAuth authentication error: {e}")
                    return None
            
            # Save the credentials for the next run
            try:
                with open(token_path, 'wb') as token:
                    pickle.dump(creds, token)
                print("{CHECKMARK} OAuth token saved for future use")
            except Exception as e:
                print(f"Warning: Could not save token: {e}")
        
        return creds
    
    def get_csv_folder_path(self) -> Path:
        """Get the path to the CSV folder. Auto-detects dated folders (e.g., daily_data_01_07_2025).
        Selects the folder with the latest date in its name."""
        csv_folder = self.config['csv_folder']
        base_path = Path(__file__).parent
        
        # If absolute path, use it; otherwise relative to script location
        if os.path.isabs(csv_folder):
            folder_path = Path(csv_folder)
        else:
            folder_path = base_path / csv_folder
        
        # If the exact folder exists, use it
        if folder_path.exists() and folder_path.is_dir():
            return folder_path
        
        # Otherwise, look for folders matching the pattern (e.g., daily_data_01_07_2025)
        base_folder_name = Path(csv_folder).name
        parent_dir = folder_path.parent if os.path.isabs(csv_folder) else base_path
        
        # Find folders that start with the base folder name
        matching_folders = [d for d in parent_dir.iterdir() 
                           if d.is_dir() and d.name.startswith(base_folder_name)]
        
        if matching_folders:
            # Extract dates from folder names and select the one with the latest date
            folders_with_dates = []
            for folder in matching_folders:
                date = self._extract_date_from_string(folder.name)
                if date:
                    folders_with_dates.append((folder, date))
            
            if folders_with_dates:
                # Sort by date (latest first)
                folders_with_dates.sort(key=lambda x: x[1], reverse=True)
                selected_folder = folders_with_dates[0][0]
                print(f"  Found dated folder: {selected_folder.name} (date: {folders_with_dates[0][1].date()})")
                return selected_folder
            else:
                # No dates found in names, use most recently modified
                matching_folders.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                selected_folder = matching_folders[0]
                print(f"  Found folder: {selected_folder.name} (no date in name, using most recent)")
                return selected_folder
        
        # Return the original path if no match found (will be created if needed)
        return folder_path
    
    def _extract_date_from_string(self, text: str) -> Optional[datetime]:
        """Helper method to extract date from a string (used for folder names)."""
        # Pattern: daily_data_MM_DD_YYYY or daily_data_MM-DD-YYYY
        date_patterns = [
            r'daily_data_(\d{2})_(\d{2})_(\d{4})',  # daily_data_01_07_2025
            r'daily_data_(\d{2})-(\d{2})-(\d{4})',  # daily_data_01-07-2025
            r'daily_data(\d{8})',  # daily_data01072025
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, text)
            if match:
                if len(match.groups()) == 3:
                    # MM_DD_YYYY or MM-DD-YYYY format
                    month, day, year = match.groups()
                    try:
                        return datetime.strptime(f"{year}-{month}-{day}", "%Y-%m-%d")
                    except ValueError:
                        continue
                elif len(match.groups()) == 1:
                    # YYYYMMDD format
                    date_str = match.group(1)
                    try:
                        return datetime.strptime(date_str, "%Y%m%d")
                    except ValueError:
                        continue
        
        return None
    
    def extract_date_from_folder_name(self) -> Optional[datetime]:
        """Extract date from folder name (e.g., daily_data_01_07_2025 -> 2025-01-07)."""
        csv_folder = self.get_csv_folder_path()
        folder_name = csv_folder.name
        return self._extract_date_from_string(folder_name)
    
    def find_csv_files(self) -> List[Path]:
        """Find all CSV files in the configured folder."""
        csv_folder = self.get_csv_folder_path()
        
        if not csv_folder.exists():
            print(f"CSV folder '{csv_folder}' does not exist. Creating it...")
            csv_folder.mkdir(parents=True, exist_ok=True)
            print(f"Please download your CSV files to: {csv_folder}")
            return []
        
        csv_files = list(csv_folder.glob("*.csv"))
        
        if not csv_files:
            print(f"No CSV files found in {csv_folder}")
            return []
        
        print(f"\nFound {len(csv_files)} CSV file(s):")
        for csv_file in sorted(csv_files):
            print(f"  - {csv_file.name}")
        
        return csv_files
    
    def extract_date_from_csv(self, csv_file: Path) -> Optional[datetime]:
        """Extract date from CSV file content."""
        try:
            df = pd.read_csv(csv_file)
            
            # Check for date column in various formats
            date_columns = [col for col in df.columns 
                          if 'date' in col.lower() or 'yyyyMMdd' in col or 'day' in col.lower()]
            
            if date_columns:
                date_col = date_columns[0]
                first_date = df[date_col].iloc[0]
                
                # Handle yyyyMMdd format (e.g., 20260106)
                if isinstance(first_date, (int, float)) and len(str(int(first_date))) == 8:
                    date_str = str(int(first_date))
                    try:
                        return datetime.strptime(date_str, "%Y%m%d")
                    except ValueError:
                        pass
                
                # Try parsing as date string
                if isinstance(first_date, str):
                    # Try common date formats
                    date_formats = ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y%m%d"]
                    for fmt in date_formats:
                        try:
                            return datetime.strptime(first_date, fmt)
                        except ValueError:
                            continue
            
            # Check filename for date pattern
            filename = csv_file.stem
            date_patterns = [
                r'(\d{4}-\d{2}-\d{2})',  # 2026-01-06
                r'(\d{8})',  # 20260106
                r'(\d{2}/\d{2}/\d{4})',  # 01/06/2026
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, filename)
                if match:
                    date_str = match.group(1)
                    # Try parsing
                    if len(date_str) == 8 and date_str.isdigit():
                        try:
                            return datetime.strptime(date_str, "%Y%m%d")
                        except ValueError:
                            pass
                    else:
                        date_formats = ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"]
                        for fmt in date_formats:
                            try:
                                return datetime.strptime(date_str, fmt)
                            except ValueError:
                                continue
            
            print(f"  Warning: Could not extract date from {csv_file.name}")
            return None
            
        except Exception as e:
            print(f"  Error reading {csv_file.name}: {e}")
            return None
    
    def get_date_column_index(self) -> Optional[int]:
        """Get the index of the date column in the Google Sheet."""
        date_col_name = self.config['google_sheet'].get('date_column')
        if not date_col_name:
            return None
        
        # Get header row (assuming row 1)
        headers = self.worksheet.row_values(1)
        
        try:
            # Try exact match first
            col_index = headers.index(date_col_name) + 1  # +1 for 1-based indexing
            return col_index
        except ValueError:
            # Try case-insensitive match
            try:
                col_index = next(i for i, h in enumerate(headers, 1) if h.lower() == date_col_name.lower())
                print(f"  Note: Found date column '{headers[col_index-1]}' (case-insensitive match)")
                return col_index
            except StopIteration:
                print(f"Warning: Date column '{date_col_name}' not found in sheet headers")
                print(f"  Available headers: {headers[:15]}...")  # Show first 15 headers
                return None
    
    def find_row_for_date(self, target_date: datetime) -> Optional[int]:
        """Find the row number for a given date in the Google Sheet."""
        date_col_index = self.get_date_column_index()
        if not date_col_index:
            return None
        
        # Get all values in the date column
        date_col = self.worksheet.col_values(date_col_index)
        
        # Format target date for comparison
        target_date_strs = [
            target_date.strftime("%Y-%m-%d"),
            target_date.strftime("%m/%d/%Y"),
            target_date.strftime("%d/%m/%Y"),
            str(int(target_date.strftime("%Y%m%d"))),
            target_date.strftime("%Y/%m/%d")
        ]
        
        # Check each row (skip header row 1)
        for row_idx, cell_value in enumerate(date_col[1:], start=2):
            if not cell_value:
                continue
            
            # Normalize cell value
            cell_str = str(cell_value).strip()
            
            # Try parsing and comparing
            for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y%m%d", "%Y/%m/%d"]:
                try:
                    cell_date = datetime.strptime(cell_str, fmt)
                    if cell_date.date() == target_date.date():
                        return row_idx
                except ValueError:
                    continue
            
            # Direct string comparison
            if cell_str in target_date_strs:
                return row_idx
        
        return None
    
    def create_row_for_date(self, target_date: datetime) -> Optional[int]:
        """Create a new row for the given date in the Google Sheet."""
        try:
            # Get the date column index
            date_col_index = self.get_date_column_index()
            if not date_col_index:
                print(f"  Error: Could not find date column '{self.config['google_sheet'].get('date_column', 'Date')}'")
                print(f"  Available headers: {self.worksheet.row_values(1)[:10]}...")  # Show first 10 headers
                return None
            
            # Get all existing rows to find where to insert
            all_values = self.worksheet.get_all_values()
            if not all_values:
                # Empty sheet, add header and first row
                headers = self.worksheet.row_values(1)
                if not headers:
                    return None
                new_row_num = 2
            else:
                # Find the right position to insert (keep dates sorted)
                date_col_values = self.worksheet.col_values(date_col_index)
                new_row_num = len(date_col_values) + 1
                
                # Try to insert in chronological order
                for row_idx, cell_value in enumerate(date_col_values[1:], start=2):
                    if not cell_value:
                        new_row_num = row_idx
                        break
                    try:
                        # Try to parse the date
                        cell_str = str(cell_value).strip()
                        for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y%m%d", "%Y/%m/%d"]:
                            try:
                                cell_date = datetime.strptime(cell_str, fmt)
                                if cell_date.date() > target_date.date():
                                    new_row_num = row_idx
                                    break
                            except ValueError:
                                continue
                        if new_row_num < len(date_col_values) + 1:
                            break
                    except:
                        continue
            
            # Determine template row (row 2 should always be the template with formulas)
            template_row = 2
            
            # Insert a new row
            self.worksheet.insert_row([], new_row_num)
            
            # After insertion, the template row might have moved
            # If we inserted at row 2, template is now at row 3
            # If we inserted after row 2, template is still at row 2
            actual_template_row = template_row if new_row_num > template_row else template_row + 1
            
            # Copy formulas from template row to the new row
            # This ensures all formulas are preserved, even for the first data row
            headers = self.worksheet.row_values(1)
            num_cols = len(headers)
            
            # Copy formulas from template row to new row using batch update
            import string
            formula_updates = []
            
            for col_idx in range(1, num_cols + 1):
                # Skip the date column - we'll set that manually
                if col_idx == date_col_index:
                    continue
                
                # Convert to A1 notation
                col_letter = ''
                col_num = col_idx
                while col_num > 0:
                    col_num -= 1
                    col_letter = string.ascii_uppercase[col_num % 26] + col_letter
                    col_num //= 26
                
                source_cell = f"{col_letter}{actual_template_row}"
                target_cell = f"{col_letter}{new_row_num}"
                
                # Get the cell value with formula
                try:
                    cell = self.worksheet.acell(source_cell, value_render_option='FORMULA')
                    if cell.value and str(cell.value).strip().startswith('='):
                        # Adjust formula references to point to the new row
                        formula = str(cell.value)
                        # Replace the template row number with the new row number in the formula
                        # This handles formulas like =IF(A2="","",TEXT(A2,"ddd")) -> =IF(A3="","",TEXT(A3,"ddd"))
                        adjusted_formula = formula.replace(f"A{actual_template_row}", f"A{new_row_num}")
                        adjusted_formula = adjusted_formula.replace(f"$A{actual_template_row}", f"$A{new_row_num}")
                        # Also replace the row number in other column references
                        import re
                        # Replace row numbers in cell references (e.g., B2, C2, etc.)
                        pattern = r'([A-Z]+\$?)(\d+)'
                        def replace_row(match):
                            col_ref = match.group(1)
                            row_num = int(match.group(2))
                            if row_num == actual_template_row:
                                return f"{col_ref}{new_row_num}"
                            return match.group(0)
                        adjusted_formula = re.sub(pattern, replace_row, adjusted_formula)
                        
                        formula_updates.append({
                            'range': target_cell,
                            'values': [[adjusted_formula]]
                        })
                except Exception as e:
                    # If we can't get the formula, skip this column
                    pass
            
            # Batch update all formulas at once
            # Use value_input_option='USER_ENTERED' to ensure formulas are interpreted as formulas, not text
            if formula_updates:
                for update in formula_updates:
                    try:
                        self.worksheet.update(
                            range_name=update['range'], 
                            values=update['values'],
                            value_input_option='USER_ENTERED'  # This ensures formulas are treated as formulas, not text
                        )
                    except Exception as e:
                        print(f"    Warning: Could not copy formula to {update['range']}: {e}")
                print(f"  {CHECKMARK} Copied {len(formula_updates)} formulas from template row {actual_template_row} to row {new_row_num}")
            
            # Set the date in the date column using direct range update (more reliable)
            date_formatted = self.format_date_for_sheet(target_date)
            # Convert column index to A1 notation (e.g., 1 -> A, 2 -> B, 27 -> AA)
            import string
            col_letter = ''
            col_num = date_col_index
            while col_num > 0:
                col_num -= 1
                col_letter = string.ascii_uppercase[col_num % 26] + col_letter
                col_num //= 26
            date_range = f"{col_letter}{new_row_num}"
            
            # Always update the date column (even if it has a formula, we want to set the actual date)
            self.worksheet.update(
                range_name=date_range, 
                values=[[date_formatted]],
                value_input_option='USER_ENTERED'
            )
            
            print(f"  {CHECKMARK} Set date '{date_formatted}' in column {date_col_index} (row {new_row_num})")
            
            return new_row_num
            
        except Exception as e:
            print(f"  Error creating row: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def map_csv_to_sheet_columns(self, csv_file: Path, target_date: datetime) -> Dict[str, any]:
        """Map CSV data to Google Sheet columns based on configuration."""
        csv_filename = csv_file.name
        mappings = self.config.get('csv_mappings', {})
        
        # Find matching mapping configuration
        mapping_config = None
        for pattern, config in mappings.items():
            if pattern in csv_filename or csv_filename in pattern:
                mapping_config = config
                break
        
        if not mapping_config:
            print(f"  Warning: No mapping configuration found for {csv_filename}")
            return {}
        
        try:
            df = pd.read_csv(csv_file)
            mapped_data = {}
            
            # Process each column mapping
            for csv_col, sheet_col in mapping_config.get('column_mappings', {}).items():
                if csv_col not in df.columns:
                    continue
                
                # Extract value based on mapping rules
                value = self.extract_value_from_csv(df, csv_col, mapping_config.get('rules', {}).get(csv_col))
                
                if value is not None:
                    mapped_data[sheet_col] = value
            
            # Handle special mappings (e.g., totals, category breakdowns)
            special_mappings = mapping_config.get('special_mappings', {})
            for rule_name, rule_config in special_mappings.items():
                values = self.apply_special_mapping(df, rule_config)
                mapped_data.update(values)
            
            return mapped_data
            
        except Exception as e:
            print(f"  Error mapping {csv_filename}: {e}")
            return {}
    
    def format_date_for_sheet(self, date: datetime) -> str:
        """Format date for Google Sheet (use format that matches existing sheet format)."""
        # Try to match existing format in sheet, default to YYYY-MM-DD
        return date.strftime("%Y-%m-%d")
    
    def extract_value_from_csv(self, df: pd.DataFrame, column: str, rule: Optional[Dict]) -> any:
        """Extract value from CSV column based on rules."""
        if column not in df.columns:
            return None
        
        if not rule:
            # Default: get first value
            return df[column].iloc[0]
        
        # Apply rule-based extraction
        rule_type = rule.get('type', 'first')
        
        if rule_type == 'first':
            return df[column].iloc[0]
        elif rule_type == 'total':
            # Find row with 'Total' in any column
            total_row = df[df.apply(lambda row: any('Total' in str(val) for val in row), axis=1)]
            if not total_row.empty:
                return total_row[column].iloc[0]
        elif rule_type == 'filter':
            # Filter by condition
            filter_col = rule.get('filter_column')
            filter_value = rule.get('filter_value')
            if filter_col and filter_col in df.columns:
                filtered = df[df[filter_col] == filter_value]
                if not filtered.empty:
                    return filtered[column].iloc[0]
        elif rule_type == 'sum':
            return df[column].sum()
        elif rule_type == 'category':
            # Extract value for specific category
            category_col = rule.get('category_column', 'Sales category')
            category_value = rule.get('category_value')
            if category_col in df.columns:
                category_row = df[df[category_col] == category_value]
                if not category_row.empty:
                    return category_row[column].iloc[0]
        
        return df[column].iloc[0]  # Fallback
    
    def apply_special_mapping(self, df: pd.DataFrame, rule_config: Dict) -> Dict[str, any]:
        """Apply special mapping rules (e.g., category breakdowns)."""
        results = {}
        
        mapping_type = rule_config.get('type')
        
        if mapping_type == 'category_breakdown':
            category_col = rule_config.get('category_column', 'Sales category')
            value_col = rule_config.get('value_column', 'Net sales')
            target_columns = rule_config.get('target_columns', {})
            
            if category_col in df.columns and value_col in df.columns:
                for category, sheet_col in target_columns.items():
                    category_row = df[df[category_col] == category]
                    if not category_row.empty:
                        results[sheet_col] = category_row[value_col].iloc[0]
        
        return results
    
    def check_existing_data(self, row_num: int) -> bool:
        """Check if row already has data (beyond just the date)."""
        if not row_num:
            return False
        
        row_values = self.worksheet.row_values(row_num)
        # Check if any cells beyond the first few have data
        return len([v for v in row_values[1:] if v]) > 0
    
    def cell_has_formula(self, row_num: int, col_index: int) -> bool:
        """Check if a cell contains a formula."""
        try:
            # Convert column index to A1 notation
            import string
            col_letter = ''
            col_num = col_index
            while col_num > 0:
                col_num -= 1
                col_letter = string.ascii_uppercase[col_num % 26] + col_letter
                col_num //= 26
            
            cell_range = f"{col_letter}{row_num}"
            
            # Try to get the cell with formula rendering
            try:
                cell = self.worksheet.acell(cell_range, value_render_option='FORMULA')
                # Check if the cell value starts with '=' (formula indicator)
                if cell.value and str(cell.value).strip().startswith('='):
                    return True
            except:
                # If that fails, try getting the cell normally and check if it's a formula
                try:
                    cell = self.worksheet.acell(cell_range)
                    # In Google Sheets API, formulas are indicated differently
                    # We can also check the cell's formulaValue property if available
                    if hasattr(cell, 'formulaValue') and cell.formulaValue:
                        return True
                except:
                    pass
            
            return False
        except Exception as e:
            # If we can't check, assume no formula to be safe (we'll update it)
            return False
    
    def update_google_sheet(self, row_num: int, data: Dict[str, any]) -> bool:
        """Update Google Sheet row with mapped data. Preserves formulas in cells.
        
        IMPORTANT: Only updates columns that are in the 'data' dictionary, which only
        contains columns specified in the config.json column_mappings. Other columns
        are never touched by this function.
        """
        if not row_num:
            print("  Error: Cannot update - row number not found")
            return False
        
        try:
            # Get column headers
            headers = self.worksheet.row_values(1)
            
            # Prepare update batch
            # NOTE: Only columns in 'data' (from column_mappings) will be updated
            # All other columns in the sheet are left untouched
            updates = []
            skipped_formulas = []
            for sheet_col, value in data.items():
                # Try exact match first, then case-insensitive
                try:
                    col_index = headers.index(sheet_col) + 1
                except ValueError:
                    # Try case-insensitive match
                    try:
                        col_index = next(i for i, h in enumerate(headers, 1) if h.lower() == sheet_col.lower())
                        print(f"  Note: Found column '{headers[col_index-1]}' for '{sheet_col}' (case-insensitive)")
                    except StopIteration:
                        print(f"  Warning: Column '{sheet_col}' not found in sheet headers")
                        continue
                
                # Check if cell has a formula - if so, skip updating it
                # Only check for formulas if the cell is not empty (to avoid false positives)
                try:
                    # Get current cell value to check if it exists
                    import string
                    check_col_letter = ''
                    check_col_num = col_index
                    while check_col_num > 0:
                        check_col_num -= 1
                        check_col_letter = string.ascii_uppercase[check_col_num % 26] + check_col_letter
                        check_col_num //= 26
                    check_range = f"{check_col_letter}{row_num}"
                    current_cell = self.worksheet.acell(check_range, value_render_option='FORMULA')
                    
                    # If cell has a formula (starts with '='), skip it
                    if current_cell.value and str(current_cell.value).strip().startswith('='):
                        skipped_formulas.append(sheet_col)
                        continue
                except:
                    # If we can't check, proceed with update (safer to update than skip)
                    pass
                
                # Convert value to appropriate format
                if pd.isna(value):
                    cell_value = ""
                elif isinstance(value, (int, float)):
                    cell_value = float(value)
                else:
                    cell_value = str(value)
                
                # Convert column index to A1 notation (e.g., 1 -> A, 2 -> B, 27 -> AA)
                import string
                col_letter = ''
                col_num = col_index
                while col_num > 0:
                    col_num -= 1
                    col_letter = string.ascii_uppercase[col_num % 26] + col_letter
                    col_num //= 26
                
                updates.append({
                    'range': f"{col_letter}{row_num}",
                    'values': [[cell_value]]
                })
            
            # Batch update
            # Use value_input_option='USER_ENTERED' to ensure proper formatting
            if updates:
                for update in updates:
                    self.worksheet.update(
                        range_name=update['range'], 
                        values=update['values'],
                        value_input_option='USER_ENTERED'
                    )
                
                print(f"  {CHECKMARK} Updated {len(updates)} columns in row {row_num}")
                if skipped_formulas:
                    print(f"  Note: Skipped {len(skipped_formulas)} columns with formulas: {', '.join(skipped_formulas[:3])}{'...' if len(skipped_formulas) > 3 else ''}")
                date_col_name = self.config['google_sheet'].get('date_column', 'Date')
                if date_col_name in data:
                    print(f"  {CHECKMARK} Date '{data[date_col_name]}' should be in row {row_num}")
                return True
            else:
                if skipped_formulas:
                    print(f"  Note: All columns had formulas, no data updated (formulas preserved)")
                else:
                    print("  Warning: No valid updates to perform")
                return False
                
        except Exception as e:
            print(f"  Error updating Google Sheet: {e}")
            return False
    
    def validate_configuration(self) -> bool:
        """Validate configuration without connecting to Google Sheets."""
        print("Validating configuration...")
        
        # Check config structure
        required_fields = ['google_sheet', 'csv_folder']
        for field in required_fields:
            if field not in self.config:
                print(f"  {CROSS} Missing required field: {field}")
                return False
            print(f"  {CHECKMARK} {field} configured")
        
        # Check Google Sheet config
        if 'sheet_id' not in self.config['google_sheet']:
            print(f"  {CROSS} Missing google_sheet.sheet_id")
            return False
        
        sheet_id = self.config['google_sheet']['sheet_id']
        if sheet_id == "YOUR_GOOGLE_SHEET_ID_HERE":
            print(f"  {CROSS} Google Sheet ID not configured (still using placeholder)")
            return False
        
        print(f"  {CHECKMARK} Google Sheet ID configured: {sheet_id[:20]}...")
        
        # Check CSV folder
        csv_folder = self.get_csv_folder_path()
        if not csv_folder.exists():
            print(f"  ⚠ CSV folder does not exist: {csv_folder}")
            print(f"     It will be created when you run the script")
        else:
            print(f"  {CHECKMARK} CSV folder found: {csv_folder.name}")
            print(f"     Full path: {csv_folder}")
        
        # Check credentials based on auth method
        auth_method = self.config.get('auth_method', 'service_account').lower()
        if auth_method == 'oauth':
            oauth_creds_path = self.config.get('oauth_credentials_file', 'oauth_credentials.json')
            if os.path.exists(oauth_creds_path):
                print(f"  {CHECKMARK} OAuth credentials file found: {oauth_creds_path}")
            else:
                print(f"  {CROSS} OAuth credentials file not found: {oauth_creds_path}")
                if not self.dry_run:
                    return False
        else:
            credentials_path = self.config.get('credentials_file', 'credentials.json')
            if os.path.exists(credentials_path):
                print(f"  {CHECKMARK} Service account credentials file found: {credentials_path}")
            else:
                print(f"  {CROSS} Service account credentials file not found: {credentials_path}")
                if not self.dry_run:
                    return False
        
        # Check mappings
        if 'csv_mappings' in self.config and self.config['csv_mappings']:
            print(f"  {CHECKMARK} {len(self.config['csv_mappings'])} CSV mappings configured")
        else:
            print(f"  ⚠ No CSV mappings configured")
        
        print("\n{CHECKMARK} Configuration validation complete!")
        return True
    
    def process_csv_files(self) -> None:
        """Main processing function - finds and processes all CSV files."""
        if self.dry_run:
            print("\n" + "="*60)
            print("DRY RUN MODE - No data will be uploaded")
            print("="*60)
            
            if not self.validate_configuration():
                print("\n⚠ Configuration validation failed. Please fix errors above.")
                return
        else:
            if not self.validate_configuration():
                print("\n⚠ Configuration validation failed. Please fix errors above.")
                return
            
            if not self.authenticate_google_sheets():
                return
        
        csv_files = self.find_csv_files()
        if not csv_files:
            return
        
        print("\n" + "="*60)
        print("Processing CSV Files")
        print("="*60)
        
        # Extract date from folder name (primary source)
        folder_date = self.extract_date_from_folder_name()
        if folder_date:
            print(f"\nExtracted date from folder name: {folder_date.date()}")
            date_key = folder_date.date()
            target_datetime = folder_date
        else:
            print("\n⚠ Could not extract date from folder name. Trying to extract from CSV files...")
            # Fallback: Group CSV files by date extracted from files
            files_by_date = {}
            for csv_file in csv_files:
                date = self.extract_date_from_csv(csv_file)
                if date:
                    date_key = date.date()
                    if date_key not in files_by_date:
                        files_by_date[date_key] = []
                    files_by_date[date_key].append(csv_file)
                else:
                    print(f"  Skipping {csv_file.name} - no date found")
            
            if not files_by_date:
                print("  Error: Could not determine date from folder name or CSV files")
                return
            
            # Use the first date found (or could use most common)
            date_key = sorted(files_by_date.keys())[0]
            target_datetime = datetime.combine(date_key, datetime.min.time())
            csv_files = files_by_date[date_key]
        
        print(f"\nProcessing date: {date_key}")
        print(f"  CSV files: {[f.name for f in csv_files]}")
        
        # Find corresponding row in Google Sheet
        if self.dry_run:
            # In dry-run mode, simulate finding the row
            print(f"  [DRY RUN] Would search for row with date {date_key}")
            row_num = 2  # Simulate row number for dry-run
        else:
            row_num = self.find_row_for_date(target_datetime)
            
            if not row_num:
                print(f"  No row found for date {date_key} in Google Sheet")
                print(f"  Creating new row for date {date_key}...")
                row_num = self.create_row_for_date(target_datetime)
                
                if not row_num:
                    print(f"  Error: Could not create row for date {date_key}")
                    return
                else:
                    print(f"  {CHECKMARK} Created new row {row_num} for date {date_key}")
        
        # Check if data already exists (skip in dry-run)
        if not self.dry_run:
            has_data = self.check_existing_data(row_num)
            
            if has_data:
                if self.config.get('overwrite_behavior', 'ask') == 'ask':
                    response = input(f"  Data already exists for {date_key}. Overwrite? (y/n): ")
                    if response.lower() != 'y':
                        print(f"  Skipping {date_key}")
                        return
                elif self.config.get('overwrite_behavior') == 'skip':
                    print(f"  Skipping {date_key} - data already exists")
                    return
        else:
            print(f"  [DRY RUN] Would check for existing data")
        
        # Process all CSV files for this date
        all_mapped_data = {}
        for csv_file in csv_files:
            print(f"\n  Processing: {csv_file.name}")
            mapped_data = self.map_csv_to_sheet_columns(csv_file, target_datetime)
            
            # Merge data (later files override earlier ones for same columns)
            all_mapped_data.update(mapped_data)
        
        # Add date to mapped data to populate Date column
        date_col_name = self.config['google_sheet'].get('date_column', 'Date')
        all_mapped_data[date_col_name] = self.format_date_for_sheet(target_datetime)
        
        # Update Google Sheet
        if all_mapped_data:
            print(f"\n  {'[DRY RUN] Would update' if self.dry_run else 'Updating'} row {row_num} with {len(all_mapped_data)} values...")
            if self.dry_run:
                print(f"  Data that would be uploaded:")
                for col, val in all_mapped_data.items():
                    print(f"    {col}: {val}")
            else:
                self.update_google_sheet(row_num, all_mapped_data)
        else:
            print(f"  No data to update for {date_key}")
        
        print("\n" + "="*60)
        print("Processing Complete!")
        print("="*60)

def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='CSV to Google Sheets Automation')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Validate configuration and show what would be uploaded without actually uploading')
    parser.add_argument('--config', default='config.json',
                       help='Path to configuration file (default: config.json)')
    
    args = parser.parse_args()
    
    print("="*60)
    print("CSV to Google Sheets Automation")
    print("="*60)
    
    try:
        automation = CSVToSheetsAutomation(config_path=args.config, dry_run=args.dry_run)
        automation.process_csv_files()
    except FileNotFoundError as e:
        print(f"\nError: {e}")
        print("Please create the configuration file. See README.md for details.")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
