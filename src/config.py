# Configuration for Traffic Management System
import os

# Project paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(BASE_DIR)
DATA_DIR = os.path.join(PROJECT_DIR, "data")
RESULTS_DIR = os.path.join(PROJECT_DIR, "results")

# File paths
RAW_DATA_FILE = "/Users/isunimayalindi/Documents/assbdp/Traffic_Collision_Data_from_2010_to_Present.csv"
PROCESSED_DATA_FILE = os.path.join(DATA_DIR, "processed_traffic_data.parquet")
CLEANED_DATA_FILE = os.path.join(DATA_DIR, "cleaned_traffic_data.parquet")

# Output files
PEAK_HOURS_OUTPUT = os.path.join(RESULTS_DIR, "01_peak_traffic_hours.csv")
CONGESTED_ROUTES_OUTPUT = os.path.join(RESULTS_DIR, "02_congested_routes.csv")
VEHICLE_FLOW_OUTPUT = os.path.join(RESULTS_DIR, "03_vehicle_flow_patterns.csv")
COLLISION_HOTSPOTS_OUTPUT = os.path.join(RESULTS_DIR, "04_collision_hotspots.csv")
TEMPORAL_ANALYSIS_OUTPUT = os.path.join(RESULTS_DIR, "05_temporal_analysis.csv")

# Spark configurations
SPARK_APP_NAME = "SmartTrafficManagement"
SPARK_MASTER = "local[*]"  # Use all available cores
SPARK_EXECUTOR_MEMORY = "4g"
SPARK_DRIVER_MEMORY = "2g"

# Data configuration
SAMPLE_SIZE = 0.1  # For testing, use 10% of data first
RANDOM_SEED = 42

# Analysis parameters
PEAK_HOUR_THRESHOLD = 0.75  # Top 25% hours are considered peak
CONGESTION_THRESHOLD = 50  # Minimum collision count to be considered congested
MIN_LOCATION_COUNT = 5  # Minimum collisions at a location

print(f"Configuration loaded from {__file__}")
print(f"Raw data path: {RAW_DATA_FILE}")
print(f"Results directory: {RESULTS_DIR}")
