"""
Simplified Data Preprocessing using Pandas
Handles data cleaning and preparation without Spark Java issues
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime

# Configuration
RAW_DATA_FILE = "/Users/isunimayalindi/Documents/assbdp/Traffic_Collision_Data_from_2010_to_Present.csv"
PROCESSED_DATA_FILE = "/Users/isunimayalindi/Documents/assbdp/smart-traffic-management/data/processed_traffic_data.csv"
RESULTS_DIR = "/Users/isunimayalindi/Documents/assbdp/smart-traffic-management/results"

print("\n" + "█" * 60)
print("█  SMART TRAFFIC MANAGEMENT - DATA PREPROCESSING  █")
print("█" * 60)

# Step 1: Load Data
print("\n" + "=" * 60)
print("STEP 1: Loading Raw Data")
print("=" * 60)

try:
    print(f"Loading from: {RAW_DATA_FILE}")
    df = pd.read_csv(RAW_DATA_FILE, low_memory=False)
    print(f"✓ Data loaded successfully")
    print(f"  Total records: {len(df):,}")
    print(f"  Columns: {len(df.columns)}")
    print(f"\nColumn Names and Types:")
    for col in df.columns[:10]:
        print(f"  - {col}: {df[col].dtype}")
    if len(df.columns) > 10:
        print(f"  ... and {len(df.columns) - 10} more columns")
except Exception as e:
    print(f"✗ Error loading data: {str(e)}")
    exit(1)

# Step 2: Data Quality Analysis
print("\n" + "=" * 60)
print("STEP 2: Data Quality Analysis")
print("=" * 60)

total_records = len(df)
print(f"\nTotal Records: {total_records:,}")

print("\nNull Value Analysis (top 10 columns):")
null_counts = df.isnull().sum()
null_pct = (null_counts / len(df)) * 100
null_df = pd.DataFrame({
    'Column': null_counts.index,
    'Null_Count': null_counts.values,
    'Null_Percentage': null_pct.values
}).sort_values('Null_Count', ascending=False).head(10)

for idx, row in null_df.iterrows():
    if row['Null_Count'] > 0:
        print(f"  - {row['Column']}: {row['Null_Count']:,} ({row['Null_Percentage']:.2f}%)")

# Step 3: Data Cleaning
print("\n" + "=" * 60)
print("STEP 3: Data Cleaning & Preprocessing")
print("=" * 60)

records_before = len(df)
print(f"Starting records: {records_before:,}")

# Remove completely null rows
df = df.dropna(how='all')
print(f"✓ Removed completely null rows")

# Remove duplicates
df_before_dedup = len(df)
df = df.drop_duplicates()
df_after_dedup = len(df)
removed_dupes = df_before_dedup - df_after_dedup
if removed_dupes > 0:
    print(f"✓ Removed {removed_dupes:,} duplicate records")

# Standardize text columns
string_cols = df.select_dtypes(include='object').columns
for col in string_cols:
    df[col] = df[col].fillna('').str.strip().str.upper()
print(f"✓ Standardized {len(string_cols)} text columns")

records_after = len(df)
print(f"Ending records: {records_after:,}")
print(f"Records removed: {records_before - records_after:,} ({((records_before - records_after) / records_before * 100):.2f}%)")

# Step 4: Feature Engineering
print("\n" + "=" * 60)
print("STEP 4: Feature Engineering")
print("=" * 60)

# Find datetime columns
date_cols = [c for c in df.columns if any(x in c.lower() for x in ['date', 'time', 'datetime', 'collision'])]
print(f"Identified temporal columns: {date_cols[:3]}")

# Process date columns
if 'Date Occurred' in df.columns:
    try:
        # Try multiple date formats
        df['Date_Occurred_parsed'] = pd.to_datetime(df['Date Occurred'], format='%m/%d/%Y', errors='coerce')
        
        # If parsing failed, try other formats
        if df['Date_Occurred_parsed'].isna().all():
            df['Date_Occurred_parsed'] = pd.to_datetime(df['Date Occurred'], errors='coerce')
        
        df['Hour'] = df['Date_Occurred_parsed'].dt.hour
        df['DayOfWeek'] = df['Date_Occurred_parsed'].dt.dayofweek + 1  # 1-7 (Monday=1, Sunday=7)
        df['Month'] = df['Date_Occurred_parsed'].dt.month
        df['Year'] = df['Date_Occurred_parsed'].dt.year
        print(f"✓ Extracted temporal features from 'Date Occurred':")
        print(f"  - Hour (0-23)")
        print(f"  - DayOfWeek (1-7)")
        print(f"  - Month (1-12)")
        print(f"  - Year")
        
        # Also extract from Time Occurred if available
        if 'Time Occurred' in df.columns:
            # Time Occurred is in HHMM format (24-hour, 0-2359)
            df['Time_Hour'] = (df['Time Occurred'] // 100).astype('Int64')
            df['Time_Hour'] = df['Time_Hour'].clip(0, 23)
            # Use Time_Hour for Hour column as it has actual time information
            df['Hour'] = df['Time_Hour'].fillna(df['Hour'])
            print(f"✓ Also extracted hour from 'Time Occurred' (HHMM format)")
    except Exception as e:
        print(f"Note: Could not parse dates: {str(e)}")

print(f"\n✓ Total columns after engineering: {len(df.columns)}")

# Step 5: Save processed data
print("\n" + "=" * 60)
print("STEP 5: Saving Processed Data")
print("=" * 60)

os.makedirs(os.path.dirname(PROCESSED_DATA_FILE), exist_ok=True)
try:
    df.to_csv(PROCESSED_DATA_FILE, index=False)
    print(f"✓ Data saved to: {PROCESSED_DATA_FILE}")
    print(f"  Format: CSV")
    print(f"  Records: {len(df):,}")
except Exception as e:
    print(f"✗ Error saving data: {str(e)}")

# Step 6: Summary
print("\n" + "=" * 60)
print("DATA SUMMARY")
print("=" * 60)
print(f"Total Records: {len(df):,}")
print(f"Total Columns: {len(df.columns)}")
print(f"Memory Usage: ~{(df.memory_usage(deep=True).sum() / (1024**2)):.2f} MB")

print("\n" + "=" * 60)
print("✓ PREPROCESSING COMPLETE!")
print("=" * 60)
print(f"\nProcessed data saved to: {PROCESSED_DATA_FILE}")
print("Ready for analysis phase.\n")
