"""
Setup script to initialize the CSV to Google Sheets automation.
Creates necessary folders and helps configure the setup.
"""

import os
import json
import shutil
from pathlib import Path

def create_csv_folder(folder_name: str = "daily_data") -> Path:
    """Create the CSV folder if it doesn't exist."""
    folder = Path(folder_name)
    folder.mkdir(parents=True, exist_ok=True)
    print(f"✓ Created CSV folder: {folder.absolute()}")
    return folder

def create_config_file() -> bool:
    """Create config.json from template if it doesn't exist."""
    config_path = Path("config.json")
    template_path = Path("config.json.template")
    
    if config_path.exists():
        print(f"⚠ config.json already exists. Skipping creation.")
        return False
    
    if not template_path.exists():
        print(f"✗ Template file {template_path} not found!")
        return False
    
    # Copy template to config.json
    shutil.copy(template_path, config_path)
    print(f"✓ Created config.json from template")
    print(f"  Please edit config.json and set your Google Sheet ID and other settings")
    return True

def check_dependencies() -> bool:
    """Check if required Python packages are installed."""
    required_packages = [
        'pandas',
        'openpyxl',
        'gspread',
        'google.auth'
    ]
    
    missing = []
    for package in required_packages:
        try:
            if package == 'google.auth':
                __import__('google.auth')
            else:
                __import__(package)
            print(f"✓ {package} is installed")
        except ImportError:
            missing.append(package)
            print(f"✗ {package} is NOT installed")
    
    if missing:
        print(f"\n⚠ Missing packages: {', '.join(missing)}")
        print(f"  Install them with: pip install -r requirements.txt")
        return False
    
    return True

def check_credentials() -> bool:
    """Check if credentials.json exists."""
    creds_path = Path("credentials.json")
    
    if creds_path.exists():
        print(f"✓ credentials.json found")
        return True
    else:
        print(f"✗ credentials.json not found")
        print(f"  Follow setup_instructions.md to create Google Sheets API credentials")
        return False

def check_xlsx_template() -> bool:
    """Check if the XLSX template file exists."""
    xlsx_file = Path("Restaurant_Daily_Ops_GSheets_Template_Targets_25_20_15.xlsx")
    
    if xlsx_file.exists():
        print(f"✓ XLSX template found: {xlsx_file.name}")
        return True
    else:
        print(f"⚠ XLSX template not found")
        return False

def main():
    """Main setup function."""
    print("="*60)
    print("CSV to Google Sheets Automation - Setup")
    print("="*60)
    print()
    
    # Check Python packages
    print("Checking Python dependencies...")
    deps_ok = check_dependencies()
    print()
    
    # Check XLSX template
    print("Checking XLSX template...")
    template_ok = check_xlsx_template()
    print()
    
    # Create CSV folder
    print("Setting up folder structure...")
    folder_name = input("Enter CSV folder name (default: daily_data): ").strip() or "daily_data"
    csv_folder = create_csv_folder(folder_name)
    print()
    
    # Create config file
    print("Setting up configuration...")
    config_created = create_config_file()
    if config_created:
        print(f"  Next steps:")
        print(f"  1. Edit config.json")
        print(f"  2. Set 'csv_folder' to: {folder_name}")
        print(f"  3. Set your Google Sheet ID")
        print(f"  4. Set the date column name")
    print()
    
    # Check credentials
    print("Checking credentials...")
    creds_ok = check_credentials()
    print()
    
    # Summary
    print("="*60)
    print("Setup Summary")
    print("="*60)
    
    if deps_ok and creds_ok and template_ok:
        print("✓ All checks passed! You're ready to use the automation.")
        print()
        print("Next steps:")
        print("1. Edit config.json with your settings")
        print("2. Place CSV files in the folder:", csv_folder.absolute())
        print("3. Run: python csv_to_sheets.py")
    else:
        print("⚠ Setup incomplete. Please address the issues above.")
        if not deps_ok:
            print("  - Install missing Python packages: pip install -r requirements.txt")
        if not creds_ok:
            print("  - Follow setup_instructions.md to create credentials.json")
        if not template_ok:
            print("  - Ensure XLSX template file is in the current directory")
    
    print()

if __name__ == "__main__":
    main()
