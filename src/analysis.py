"""
Traffic Analysis Module
Purpose: Perform detailed analysis on traffic data to identify patterns
Design Patterns Used: Aggregation, Sorting, Filtering, Join Operations
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, count, avg, max, min, sum as spark_sum, 
    rank, dense_rank, row_number, desc, asc,
    coalesce, round as spark_round
)
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
from config import *

def create_spark_session():
    """Create and configure Spark session"""
    print("Initializing Spark Session for Analysis...")
    
    spark = SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .master(SPARK_MASTER) \
        .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY) \
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def load_processed_data(spark, file_path):
    """Load processed data"""
    print(f"Loading processed data from: {file_path}")
    try:
        df = spark.read.parquet(file_path)
        print(f"✓ Data loaded: {df.count():,} records")
        return df
    except Exception as e:
        print(f"✗ Error loading data: {str(e)}")
        return None

def analyze_peak_traffic_hours(df):
    """
    DESIGN PATTERN: Aggregation + Sorting
    Identify peak traffic hours
    """
    print("\n" + "=" * 60)
    print("ANALYSIS 1: Peak Traffic Hours")
    print("=" * 60)
    
    if "Hour" not in df.columns:
        print("✗ Hour column not found")
        return None
    
    # Aggregation: Group by hour and count collisions
    hourly_stats = df.groupBy("Hour").agg(
        count("*").alias("CollisionCount"),
        count(col("Hour")).alias("TotalIncidents")
    ).withColumn(
        "Percentage", spark_round((col("CollisionCount") / df.count() * 100), 2)
    )
    
    # Sorting: Sort by collision count descending
    hourly_stats = hourly_stats.sort(desc("CollisionCount"))
    
    # Identify peak hours (top 25%)
    total_hours = hourly_stats.count()
    peak_threshold = int(total_hours * (1 - PEAK_HOUR_THRESHOLD))
    
    peak_hours = hourly_stats.limit(peak_threshold)
    
    print(f"\nPeak Traffic Hours (Top {peak_threshold} hours):")
    peak_hours.show(truncate=False)
    
    # Create time-based categories
    peak_hours_with_category = peak_hours.withColumn(
        "TimeCategory", 
        col("Percentage")
    )
    
    return hourly_stats

def analyze_congested_routes(df):
    """
    DESIGN PATTERN: Filtering + Aggregation
    Detect congested routes/locations
    """
    print("\n" + "=" * 60)
    print("ANALYSIS 2: Congested Routes & Locations")
    print("=" * 60)
    
    # Find location column
    location_col = None
    for col_name in df.columns:
        if any(x in col_name.lower() for x in ['street', 'location', 'address', 'area']):
            location_col = col_name
            break
    
    if not location_col:
        print("✗ No location column found")
        return None
    
    # Filtering + Aggregation: Group by location and filter
    location_stats = df.groupBy(location_col).agg(
        count("*").alias("CollisionCount"),
        count(col("Hour")).alias("IncidentCount")
    ).filter(
        col("CollisionCount") >= MIN_LOCATION_COUNT  # Filtering pattern
    ).withColumn(
        "Percentage", spark_round((col("CollisionCount") / df.count() * 100), 2)
    )
    
    # Sorting: Sort by collision count
    location_stats = location_stats.sort(desc("CollisionCount"))
    
    # Identify congested routes
    congested = location_stats.filter(col("CollisionCount") >= CONGESTION_THRESHOLD)
    
    print(f"\nTop Congested Routes (≥{CONGESTION_THRESHOLD} collisions):")
    congested.show(20, truncate=False)
    
    print(f"\nTotal locations analyzed: {location_stats.count()}")
    print(f"Highly congested locations (≥{CONGESTION_THRESHOLD}): {congested.count()}")
    
    return location_stats

def analyze_vehicle_flow_patterns(df):
    """
    DESIGN PATTERN: Aggregation + Time-based Analysis
    Analyze vehicle flow patterns by hour and day
    """
    print("\n" + "=" * 60)
    print("ANALYSIS 3: Vehicle Flow Patterns")
    print("=" * 60)
    
    if "Hour" not in df.columns or "DayOfWeek" not in df.columns:
        print("✗ Required temporal columns not found")
        return None
    
    # Aggregation: Group by hour and day of week
    flow_patterns = df.groupBy("Hour", "DayOfWeek").agg(
        count("*").alias("CollisionCount")
    ).withColumn(
        "DayName", 
        col("DayOfWeek").cast("string")  # Will be replaced with proper logic
    )
    
    # Add day names mapping
    day_names = {
        "1": "Sunday",
        "2": "Monday", 
        "3": "Tuesday",
        "4": "Wednesday",
        "5": "Thursday",
        "6": "Friday",
        "7": "Saturday"
    }
    
    # Sorting
    flow_patterns = flow_patterns.sort(desc("CollisionCount"))
    
    print("\nTop 30 Traffic Flow Patterns (Hour x Day of Week):")
    flow_patterns.limit(30).show(truncate=False)
    
    # Hourly pattern summary
    hourly_flow = df.groupBy("Hour").agg(
        count("*").alias("DailyAverageCollisions")
    ).sort(asc("Hour"))
    
    print("\nAverage Collisions by Hour of Day:")
    hourly_flow.show(24, truncate=False)
    
    return flow_patterns

def analyze_collision_hotspots(df):
    """
    DESIGN PATTERN: Filtering + Aggregation + Sorting
    Identify collision hotspots combining location and time
    """
    print("\n" + "=" * 60)
    print("ANALYSIS 4: Collision Hotspots")
    print("=" * 60)
    
    location_col = None
    for col_name in df.columns:
        if any(x in col_name.lower() for x in ['street', 'location', 'address', 'area']):
            location_col = col_name
            break
    
    if not location_col or "Hour" not in df.columns:
        print("✗ Required columns not found")
        return None
    
    # Filtering: Focus on peak hours (7-9 AM, 4-6 PM)
    peak_hour_data = df.filter(col("Hour").isin([7, 8, 9, 16, 17, 18]))
    
    # Aggregation: Combine location and time
    hotspots = peak_hour_data.groupBy(location_col, "Hour").agg(
        count("*").alias("PeakHourCollisions")
    ).filter(
        col("PeakHourCollisions") > 0
    )
    
    # Sorting: Sort by collision count
    hotspots = hotspots.sort(desc("PeakHourCollisions"))
    
    print("\nTop Collision Hotspots During Peak Hours (7-9 AM, 4-6 PM):")
    hotspots.limit(20).show(truncate=False)
    
    return hotspots

def analyze_temporal_trends(df):
    """
    DESIGN PATTERN: Time-series Aggregation
    Analyze temporal trends over months and years
    """
    print("\n" + "=" * 60)
    print("ANALYSIS 5: Temporal Trends")
    print("=" * 60)
    
    if "Month" not in df.columns or "Year" not in df.columns:
        print("✗ Temporal columns not found")
        return None
    
    # Monthly trends
    monthly_trends = df.groupBy("Year", "Month").agg(
        count("*").alias("MonthlyCollisions")
    ).sort(desc("Year"), desc("Month"))
    
    print("\nMonthly Collision Trends:")
    monthly_trends.limit(24).show(truncate=False)
    
    # Yearly summary
    yearly_summary = df.groupBy("Year").agg(
        count("*").alias("YearlyCollisions"),
        spark_round(avg(col("Hour")), 2).alias("AvgHourOfDay")
    ).sort(desc("Year"))
    
    print("\nYearly Summary:")
    yearly_summary.show(truncate=False)
    
    return monthly_trends

def save_results(df, output_path, analysis_name):
    """Save analysis results"""
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
        print(f"✓ {analysis_name} saved to: {output_path}")
        return True
    except Exception as e:
        print(f"✗ Error saving {analysis_name}: {str(e)}")
        return False

def main():
    """Main analysis pipeline"""
    print("\n")
    print("█" * 60)
    print("█  SMART TRAFFIC MANAGEMENT - ANALYSIS  █")
    print("█" * 60)
    
    spark = create_spark_session()
    
    try:
        # Load processed data
        df = load_processed_data(spark, PROCESSED_DATA_FILE)
        if df is None:
            print("Cannot proceed without data")
            return
        
        # Create output directory
        os.makedirs(RESULTS_DIR, exist_ok=True)
        
        # Analysis 1: Peak Traffic Hours
        peak_hours = analyze_peak_traffic_hours(df)
        if peak_hours:
            save_results(peak_hours, PEAK_HOURS_OUTPUT, "Peak Hours Analysis")
        
        # Analysis 2: Congested Routes
        congested_routes = analyze_congested_routes(df)
        if congested_routes:
            save_results(congested_routes, CONGESTED_ROUTES_OUTPUT, "Congested Routes")
        
        # Analysis 3: Vehicle Flow Patterns
        flow_patterns = analyze_vehicle_flow_patterns(df)
        if flow_patterns:
            save_results(flow_patterns, VEHICLE_FLOW_OUTPUT, "Vehicle Flow Patterns")
        
        # Analysis 4: Collision Hotspots
        hotspots = analyze_collision_hotspots(df)
        if hotspots:
            save_results(hotspots, COLLISION_HOTSPOTS_OUTPUT, "Collision Hotspots")
        
        # Analysis 5: Temporal Trends
        temporal = analyze_temporal_trends(df)
        if temporal:
            save_results(temporal, TEMPORAL_ANALYSIS_OUTPUT, "Temporal Analysis")
        
        print("\n" + "=" * 60)
        print("✓ ALL ANALYSES COMPLETE!")
        print("=" * 60)
        print(f"\nResults saved to: {RESULTS_DIR}")
        print("\nGenerated files:")
        print(f"  1. {os.path.basename(PEAK_HOURS_OUTPUT)}")
        print(f"  2. {os.path.basename(CONGESTED_ROUTES_OUTPUT)}")
        print(f"  3. {os.path.basename(VEHICLE_FLOW_OUTPUT)}")
        print(f"  4. {os.path.basename(COLLISION_HOTSPOTS_OUTPUT)}")
        print(f"  5. {os.path.basename(TEMPORAL_ANALYSIS_OUTPUT)}")
        
    except Exception as e:
        print(f"\n✗ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
