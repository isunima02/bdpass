"""
Data Preprocessing Module
Purpose: Clean, transform, and prepare traffic collision data for analysis
Design Patterns Used: Filtering, Data Transformation
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, hour, dayofweek, month, year,
    when, trim, upper, isnull, count, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import os
import sys

# Import configuration
sys.path.insert(0, os.path.dirname(__file__))
from config import *

def create_spark_session():
    """Create and configure Spark session"""
    print("=" * 60)
    print("Initializing Spark Session...")
    print("=" * 60)
    
    spark = SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .master(SPARK_MASTER) \
        .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY) \
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print(f"✓ Spark Session created successfully")
    print(f"  App Name: {SPARK_APP_NAME}")
    print(f"  Master: {SPARK_MASTER}")
    return spark

def load_raw_data(spark, file_path):
    """Load raw CSV data into DataFrame"""
    print("\n" + "=" * 60)
    print("STEP 1: Loading Raw Data")
    print("=" * 60)
    
    try:
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        print(f"✓ Data loaded successfully from: {file_path}")
        print(f"  Total records: {df.count():,}")
        print(f"  Columns: {len(df.columns)}")
        print(f"\nColumn Names and Types:")
        for field in df.schema.fields:
            print(f"  - {field.name}: {field.dataType}")
        return df
    except Exception as e:
        print(f"✗ Error loading data: {str(e)}")
        return None

def analyze_data_quality(df):
    """Analyze data quality issues"""
    print("\n" + "=" * 60)
    print("STEP 2: Data Quality Analysis")
    print("=" * 60)
    
    total_records = df.count()
    print(f"\nTotal Records: {total_records:,}")
    
    # Check for nulls
    print("\nNull Value Analysis:")
    null_counts = df.select([count(when(isnull(c), c)).alias(c) for c in df.columns])
    null_data = null_counts.collect()[0].asDict()
    for col_name, null_count in sorted(null_data.items(), key=lambda x: x[1], reverse=True):
        if null_count > 0:
            null_pct = (null_count / total_records) * 100
            print(f"  - {col_name}: {null_count:,} ({null_pct:.2f}%)")
    
    # Show sample records
    print("\nSample Records (first 5):")
    df.show(5, truncate=False)
    
    return df

def clean_and_preprocess(df):
    """Clean and preprocess data"""
    print("\n" + "=" * 60)
    print("STEP 3: Data Cleaning & Preprocessing")
    print("=" * 60)
    
    records_before = df.count()
    print(f"Starting records: {records_before:,}")
    
    # 1. Remove completely null rows
    df = df.dropna(how='all')
    print(f"✓ Removed completely null rows")
    
    # 2. Select relevant columns for traffic analysis
    # Common traffic-related columns (adjust based on actual CSV structure)
    relevant_cols = [col for col in df.columns if col.lower() not in 
                    ['report_id', 'case_id', 'dr_number']]
    df = df.select([c for c in relevant_cols if c in df.columns])
    
    # 3. Standardize text columns
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for col_name in string_cols:
        df = df.withColumn(col_name, trim(upper(col_name)))
    
    # 4. Handle date/time columns - look for common datetime column names
    date_cols = [c for c in df.columns if any(x in c.lower() for x in ['date', 'time', 'datetime'])]
    
    for date_col in date_cols:
        try:
            # Try to convert to timestamp
            df = df.withColumn(f"{date_col}_parsed", to_timestamp(col(date_col)))
            if df.select(f"{date_col}_parsed").filter(col(f"{date_col}_parsed").isNotNull()).count() > 0:
                df = df.drop(date_col).withColumnRenamed(f"{date_col}_parsed", date_col)
                print(f"✓ Converted {date_col} to timestamp")
        except:
            pass
    
    # 5. Remove duplicates
    df_before_dedup = df.count()
    df = df.dropDuplicates()
    df_after_dedup = df.count()
    removed_dupes = df_before_dedup - df_after_dedup
    if removed_dupes > 0:
        print(f"✓ Removed {removed_dupes:,} duplicate records")
    
    records_after = df.count()
    print(f"Ending records: {records_after:,}")
    print(f"Records removed: {records_before - records_after:,} ({((records_before - records_after) / records_before * 100):.2f}%)")
    
    return df

def feature_engineering(df):
    """Create features for analysis"""
    print("\n" + "=" * 60)
    print("STEP 4: Feature Engineering")
    print("=" * 60)
    
    # Find timestamp column
    timestamp_col = None
    for col_name in df.columns:
        if any(x in col_name.lower() for x in ['date', 'time', 'datetime']):
            timestamp_col = col_name
            break
    
    if timestamp_col:
        # Extract time-based features
        df = df.withColumn("Hour", hour(col(timestamp_col)))
        df = df.withColumn("DayOfWeek", dayofweek(col(timestamp_col)))
        df = df.withColumn("Month", month(col(timestamp_col)))
        df = df.withColumn("Year", year(col(timestamp_col)))
        
        print(f"✓ Extracted temporal features from '{timestamp_col}':")
        print(f"  - Hour (0-23)")
        print(f"  - DayOfWeek (1=Sunday, 7=Saturday)")
        print(f"  - Month (1-12)")
        print(f"  - Year")
        
        # Create peak hour indicator
        df = df.withColumn("IsPeakHour", when(col("Hour").isin(7,8,9,16,17,18), 1).otherwise(0))
        print(f"✓ Created IsPeakHour indicator")
    else:
        print("✗ No timestamp column found for feature engineering")
    
    print(f"\nFinal columns count: {len(df.columns)}")
    
    return df

def save_processed_data(df, output_path):
    """Save processed data in Parquet format"""
    print("\n" + "=" * 60)
    print("STEP 5: Saving Processed Data")
    print("=" * 60)
    
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        df.write.mode("overwrite").parquet(output_path)
        print(f"✓ Data saved to: {output_path}")
        print(f"  Format: Parquet (optimized for Spark)")
        print(f"  Records: {df.count():,}")
        print(f"  Partitions: {df.rdd.getNumPartitions()}")
        return True
    except Exception as e:
        print(f"✗ Error saving data: {str(e)}")
        return False

def print_summary(df):
    """Print data summary"""
    print("\n" + "=" * 60)
    print("DATA SUMMARY")
    print("=" * 60)
    print(f"Total Records: {df.count():,}")
    print(f"Total Columns: {len(df.columns)}")
    print(f"\nColumns:")
    for i, col_name in enumerate(df.columns, 1):
        print(f"  {i}. {col_name}")

def main():
    """Main preprocessing pipeline"""
    print("\n")
    print("█" * 60)
    print("█  SMART TRAFFIC MANAGEMENT - DATA PREPROCESSING  █")
    print("█" * 60)
    
    # Step 1: Create Spark session
    spark = create_spark_session()
    
    try:
        # Step 2: Load data
        df = load_raw_data(spark, RAW_DATA_FILE)
        if df is None:
            return
        
        # Step 3: Analyze quality
        df = analyze_data_quality(df)
        
        # Step 4: Clean data
        df = clean_and_preprocess(df)
        
        # Step 5: Feature engineering
        df = feature_engineering(df)
        
        # Step 6: Save processed data
        save_processed_data(df, PROCESSED_DATA_FILE)
        
        # Step 7: Print summary
        print_summary(df)
        
        print("\n" + "=" * 60)
        print("✓ PREPROCESSING COMPLETE!")
        print("=" * 60)
        print(f"\nProcessed data saved to: {PROCESSED_DATA_FILE}")
        print("Ready for analysis phase.\n")
        
    except Exception as e:
        print(f"\n✗ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
