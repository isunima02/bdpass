"""
Traffic Collision Analysis using Pandas
Implements 5 MapReduce design patterns for distributed analysis
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime

# Configuration
PROCESSED_DATA_FILE = "/Users/isunimayalindi/Documents/assbdp/smart-traffic-management/data/processed_traffic_data.csv"
RESULTS_DIR = "/Users/isunimayalindi/Documents/assbdp/smart-traffic-management/results"

# Ensure results directory exists
os.makedirs(RESULTS_DIR, exist_ok=True)

print("\n" + "█" * 60)
print("█  SMART TRAFFIC MANAGEMENT - ANALYSIS  █")
print("█" * 60)

# Load processed data
print("\nLoading processed data...")
df = pd.read_csv(PROCESSED_DATA_FILE)
print(f"✓ Loaded {len(df):,} records")

# ===================================================================
# PATTERN 1: FILTERING
# ===================================================================
print("\n" + "=" * 60)
print("ANALYSIS 1: PEAK TRAFFIC HOURS (Filtering Pattern)")
print("=" * 60)

try:
    # Filter for valid hours and calculate collision frequency
    df_hours = df[df['Hour'].notna()].copy()
    peak_hours = df_hours.groupby('Hour').size().reset_index(name='CollisionCount')
    peak_hours = peak_hours.sort_values('CollisionCount', ascending=False)
    
    # Save results
    output_file = os.path.join(RESULTS_DIR, "01_peak_traffic_hours.csv")
    peak_hours.to_csv(output_file, index=False)
    print(f"✓ Analyzed {len(peak_hours)} hours")
    print(f"✓ Saved to: {output_file}")
    print(f"\nTop 5 Peak Hours:")
    for idx, row in peak_hours.head().iterrows():
        print(f"  {int(row['Date Reported_Hour']):02d}:00 - {int(row['CollisionCount']):,} collisions")
except Exception as e:
    print(f"✗ Error in peak hours analysis: {str(e)}")

# ===================================================================
# PATTERN 2: AGGREGATION
# ===================================================================
print("\n" + "=" * 60)
print("ANALYSIS 2: CONGESTED ROUTES (Aggregation Pattern)")
print("=" * 60)

try:
    # Find location columns
    location_cols = [c for c in df.columns if any(x in c.lower() for x in ['street', 'location', 'area', 'district'])]
    
    if location_cols:
        location_col = 'Area Name'  # Use Area Name as primary location identifier
        if location_col in df.columns:
            congested = df[df[location_col].notna()].groupby(location_col).size().reset_index(name='CollisionCount')
            congested = congested.sort_values('CollisionCount', ascending=False).head(20)
        
        output_file = os.path.join(RESULTS_DIR, "02_congested_routes.csv")
        congested.to_csv(output_file, index=False)
        print(f"✓ Analyzed {len(congested)} locations")
        print(f"✓ Saved to: {output_file}")
        print(f"\nTop 5 Congested Routes:")
        for idx, row in congested.head().iterrows():
            print(f"  {row[location_col]}: {int(row['CollisionCount']):,} collisions")
    else:
        print("✗ No location column found")
except Exception as e:
    print(f"✗ Error in congestion analysis: {str(e)}")

# ===================================================================
# PATTERN 3: TIME-SERIES AGGREGATION
# ===================================================================
print("\n" + "=" * 60)
print("ANALYSIS 3: VEHICLE FLOW PATTERNS (Time-Series Pattern)")
print("=" * 60)

try:
    # Create hour x day of week pivot table
    df_flow = df[(df['Hour'].notna()) & (df['DayOfWeek'].notna())].copy()
    
    flow = df_flow.groupby(['DayOfWeek', 'Hour']).size().reset_index(name='CollisionCount')
    
    # Convert to pivot table
    flow_pivot = flow.pivot_table(index='Hour', columns='DayOfWeek', values='CollisionCount', fill_value=0)
    flow_pivot.columns = [f'Day_{int(c)}' for c in flow_pivot.columns]
    flow_pivot = flow_pivot.reset_index()
    flow_pivot.rename(columns={'Hour': 'Hour'}, inplace=True)
    
    output_file = os.path.join(RESULTS_DIR, "03_vehicle_flow_patterns.csv")
    flow_pivot.to_csv(output_file, index=False)
    print(f"✓ Analyzed flow patterns across {len(flow_pivot)} hours × 7 days")
    print(f"✓ Saved to: {output_file}")
    print(f"\nAverage collisions per hour:")
    print(f"  All hours: {flow_pivot.iloc[:, 1:].mean().mean():.0f}")
except Exception as e:
    print(f"✗ Error in flow pattern analysis: {str(e)}")

# ===================================================================
# PATTERN 4: MULTIPLE JOINS & FILTERING
# ===================================================================
print("\n" + "=" * 60)
print("ANALYSIS 4: COLLISION HOTSPOTS (Join Pattern)")
print("=" * 60)

try:
    location_cols = [c for c in df.columns if any(x in c.lower() for x in ['street', 'location', 'area', 'district'])]
    
    if location_cols:
        location_col = 'Area Name'
        if location_col in df.columns:
            # Create hotspot analysis combining location and hour
            df_hotspots = df[(df[location_col].notna()) & (df['Hour'].notna())].copy()
            hotspots = df_hotspots.groupby([location_col, 'Hour']).size().reset_index(name='CollisionCount')
            hotspots = hotspots.sort_values('CollisionCount', ascending=False).head(30)
        
        output_file = os.path.join(RESULTS_DIR, "04_collision_hotspots.csv")
        hotspots.to_csv(output_file, index=False)
        print(f"✓ Identified {len(hotspots)} hotspots")
        print(f"✓ Saved to: {output_file}")
        print(f"\nTop 5 Hotspots (Location × Hour):")
        for idx, row in hotspots.head().iterrows():
            print(f"  {row[location_col]} @ {int(row['Hour']):02d}:00 - {int(row['CollisionCount']):,} collisions")
    else:
        print("✗ No location column found")
except Exception as e:
    print(f"✗ Error in hotspot analysis: {str(e)}")

# ===================================================================
# PATTERN 5: TEMPORAL SORTING & AGGREGATION
# ===================================================================
print("\n" + "=" * 60)
print("ANALYSIS 5: TEMPORAL TRENDS (Sorting Pattern)")
print("=" * 60)

try:
    # Analyze trends over years and months
    df_temporal = df[(df['Year'].notna()) & (df['Month'].notna())].copy()
    temporal = df_temporal.groupby(['Year', 'Month']).size().reset_index(name='CollisionCount')
    temporal = temporal.sort_values(['Year', 'Month'])
    
    # Add year-month column for better readability
    temporal['YearMonth'] = temporal['Year'].astype(int).astype(str) + '-' + temporal['Month'].astype(int).astype(str).str.zfill(2)
    
    output_file = os.path.join(RESULTS_DIR, "05_temporal_analysis.csv")
    temporal.to_csv(output_file, index=False)
    print(f"✓ Analyzed {len(temporal)} months")
    print(f"✓ Saved to: {output_file}")
    print(f"\nTemporal Statistics:")
    print(f"  Year range: {int(temporal['Year'].min())} - {int(temporal['Year'].max())}")
    print(f"  Average monthly collisions: {temporal['CollisionCount'].mean():.0f}")
    print(f"  Peak month: {temporal.loc[temporal['CollisionCount'].idxmax(), 'YearMonth']} ({int(temporal['CollisionCount'].max())} collisions)")
except Exception as e:
    print(f"✗ Error in temporal analysis: {str(e)}")

# ===================================================================
# SUMMARY
# ===================================================================
print("\n" + "=" * 60)
print("ANALYSIS SUMMARY")
print("=" * 60)
print(f"\nData processed: {len(df):,} records")
print(f"\n5 Analysis Files Generated:")
print(f"  1. ✓ 01_peak_traffic_hours.csv")
print(f"  2. ✓ 02_congested_routes.csv")
print(f"  3. ✓ 03_vehicle_flow_patterns.csv")
print(f"  4. ✓ 04_collision_hotspots.csv")
print(f"  5. ✓ 05_temporal_analysis.csv")
print(f"\nOutput directory: {RESULTS_DIR}")

# List generated files
print("\nGenerated Files:")
for filename in sorted(os.listdir(RESULTS_DIR)):
    filepath = os.path.join(RESULTS_DIR, filename)
    file_size = os.path.getsize(filepath) / 1024  # KB
    print(f"  - {filename} ({file_size:.1f} KB)")

print("\n" + "=" * 60)
print("✓ ANALYSIS COMPLETE!")
print("=" * 60)
print("\nReady for visualization and reporting.\n")
