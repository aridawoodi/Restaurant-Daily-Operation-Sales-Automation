"""
Analysis script to understand the XLSX template structure and CSV mapping.
This script reads the main XLSX file and analyzes the 'daily ops' sheet structure.
"""

import pandas as pd
import os
from pathlib import Path
import json

def analyze_xlsx_structure():
    """Analyze the main XLSX file to understand the 'daily ops' sheet structure."""
    xlsx_path = "Restaurant_Daily_Ops_GSheets_Template_Targets_25_20_15.xlsx"
    
    if not os.path.exists(xlsx_path):
        print(f"Error: {xlsx_path} not found!")
        return None
    
    print(f"Reading {xlsx_path}...")
    xls = pd.ExcelFile(xlsx_path)
    
    print(f"\nAvailable sheets: {xls.sheet_names}")
    
    if 'daily ops' not in xls.sheet_names:
        print("\nWarning: 'daily ops' sheet not found. Available sheets:")
        for sheet in xls.sheet_names:
            print(f"  - {sheet}")
        return None
    
    print("\nAnalyzing 'daily ops' sheet...")
    df = pd.read_excel(xls, sheet_name='daily ops')
    
    print(f"\nSheet dimensions: {df.shape[0]} rows x {df.shape[1]} columns")
    print(f"\nColumn names ({len(df.columns)} total):")
    for i, col in enumerate(df.columns, 1):
        print(f"  {i:2d}. {col}")
    
    # Show first few rows to understand structure
    print("\nFirst 5 rows of data:")
    print(df.head().to_string())
    
    # Check for date column
    date_columns = [col for col in df.columns if 'date' in col.lower() or 'day' in col.lower()]
    if date_columns:
        print(f"\nPotential date columns found: {date_columns}")
    
    # Check data types
    print("\nColumn data types:")
    print(df.dtypes)
    
    # Look for formulas or calculated columns (rows with formulas might be empty in pandas)
    print("\nSample data from each column (first non-null value):")
    for col in df.columns:
        first_val = df[col].dropna().iloc[0] if not df[col].dropna().empty else "N/A"
        print(f"  {col}: {first_val}")
    
    return df

def analyze_csv_files():
    """Analyze all CSV files in the current directory to understand their structure."""
    csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]
    
    print(f"\n\nFound {len(csv_files)} CSV files:")
    csv_analysis = {}
    
    for csv_file in sorted(csv_files):
        print(f"\n{'='*60}")
        print(f"Analyzing: {csv_file}")
        print('='*60)
        
        try:
            df = pd.read_csv(csv_file)
            csv_analysis[csv_file] = {
                'columns': list(df.columns),
                'shape': df.shape,
                'first_row': df.iloc[0].to_dict() if not df.empty else {}
            }
            
            print(f"  Dimensions: {df.shape[0]} rows x {df.shape[1]} columns")
            print(f"  Columns: {list(df.columns)}")
            
            # Check for date-like columns
            date_cols = [col for col in df.columns if 'date' in col.lower() or 'day' in col.lower()]
            if date_cols:
                print(f"  Date columns: {date_cols}")
            
            # Show first row
            if not df.empty:
                print(f"  First row sample:")
                for col, val in df.iloc[0].items():
                    print(f"    {col}: {val}")
        
        except Exception as e:
            print(f"  Error reading {csv_file}: {e}")
            csv_analysis[csv_file] = {'error': str(e)}
    
    return csv_analysis

def generate_mapping_config(daily_ops_df, csv_analysis):
    """Generate a suggested mapping configuration based on analysis."""
    config = {
        "google_sheet": {
            "sheet_name": "daily ops",
            "date_column": None  # Will be determined
        },
        "csv_folder": "daily_data",  # Default subfolder name
        "csv_mappings": {}
    }
    
    # Try to find date column in daily ops
    date_cols = [col for col in daily_ops_df.columns if 'date' in col.lower() or 'day' in col.lower()]
    if date_cols:
        config["google_sheet"]["date_column"] = date_cols[0]
    
    # Suggest mappings based on CSV file names and columns
    for csv_file, analysis in csv_analysis.items():
        if 'error' in analysis:
            continue
        
        mapping = {
            "file_pattern": csv_file.replace('.csv', ''),
            "columns": analysis['columns'],
            "suggested_mappings": {}
        }
        
        # Try to match CSV columns to daily ops columns
        for csv_col in analysis['columns']:
            # Look for similar column names
            matches = [col for col in daily_ops_df.columns 
                      if csv_col.lower() in col.lower() or col.lower() in csv_col.lower()]
            if matches:
                mapping["suggested_mappings"][csv_col] = matches[0]
        
        config["csv_mappings"][csv_file] = mapping
    
    return config

def main():
    print("="*60)
    print("XLSX and CSV Structure Analysis")
    print("="*60)
    
    # Analyze XLSX
    daily_ops_df = analyze_xlsx_structure()
    
    if daily_ops_df is None:
        print("\nCould not analyze XLSX file. Exiting.")
        return
    
    # Analyze CSV files
    csv_analysis = analyze_csv_files()
    
    # Generate mapping suggestion
    print("\n\n" + "="*60)
    print("Generating Mapping Configuration Suggestion")
    print("="*60)
    
    config = generate_mapping_config(daily_ops_df, csv_analysis)
    
    # Save analysis results
    output_file = "structure_analysis.json"
    output = {
        "daily_ops_columns": list(daily_ops_df.columns),
        "daily_ops_shape": list(daily_ops_df.shape),
        "csv_analysis": csv_analysis,
        "suggested_config": config
    }
    
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2, default=str)
    
    print(f"\nAnalysis complete! Results saved to {output_file}")
    print("\nSuggested mapping configuration:")
    print(json.dumps(config, indent=2, default=str))

if __name__ == "__main__":
    main()
