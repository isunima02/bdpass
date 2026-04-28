"""
Utility Functions Module
Purpose: Common utility functions for the project
"""

from datetime import datetime
import os

def print_header(title):
    """Print formatted header"""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)

def print_section(title):
    """Print formatted section"""
    print("\n" + "-" * 70)
    print(f"  {title}")
    print("-" * 70)

def print_result(message, success=True):
    """Print formatted result"""
    symbol = "✓" if success else "✗"
    print(f"{symbol} {message}")

def ensure_directory(path):
    """Ensure directory exists"""
    os.makedirs(path, exist_ok=True)
    return path

def get_timestamp():
    """Get current timestamp as string"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def format_number(num):
    """Format large numbers with commas"""
    return f"{num:,.0f}"

def calculate_percentage(part, total):
    """Calculate percentage"""
    if total == 0:
        return 0
    return (part / total) * 100

def print_metrics(metrics_dict):
    """Print metrics dictionary"""
    for key, value in metrics_dict.items():
        print(f"  {key}: {value}")

def save_json_metadata(output_path, metadata):
    """Save metadata as JSON"""
    import json
    with open(output_path, 'w') as f:
        json.dump(metadata, f, indent=2)

class AnalysisLogger:
    """Logger for analysis operations"""
    
    def __init__(self, log_file=None):
        self.log_file = log_file
        self.logs = []
    
    def log(self, message, level="INFO"):
        """Log message"""
        timestamp = get_timestamp()
        log_entry = f"[{timestamp}] [{level}] {message}"
        self.logs.append(log_entry)
        print(log_entry)
        
        if self.log_file:
            with open(self.log_file, 'a') as f:
                f.write(log_entry + "\n")
    
    def get_logs(self):
        """Get all logs"""
        return self.logs

# Design Patterns Documentation
DESIGN_PATTERNS = {
    "FILTERING": {
        "description": "Filter rows based on conditions",
        "use_case": "Select collisions in specific time periods or locations",
        "example": "df.filter(col('Hour') >= 7)"
    },
    "AGGREGATION": {
        "description": "Group and aggregate data",
        "use_case": "Count collisions by hour, location, or other dimensions",
        "example": "df.groupBy('Hour').agg(count('*'))"
    },
    "SORTING": {
        "description": "Sort data by one or more columns",
        "use_case": "Rank locations by collision frequency",
        "example": "df.sort(desc('CollisionCount'))"
    },
    "JOIN": {
        "description": "Combine datasets based on common keys",
        "use_case": "Join collision data with location metadata",
        "example": "df1.join(df2, 'LocationID')"
    },
    "TIME_SERIES": {
        "description": "Analyze data across time periods",
        "use_case": "Track collision trends over months/years",
        "example": "df.groupBy('Month').agg(count('*'))"
    }
}

def print_design_patterns():
    """Print design patterns used"""
    print("\n" + "=" * 70)
    print("  MAPREDUCE DESIGN PATTERNS USED")
    print("=" * 70)
    
    for pattern_name, pattern_info in DESIGN_PATTERNS.items():
        print(f"\n{pattern_name}")
        print(f"  Description: {pattern_info['description']}")
        print(f"  Use Case: {pattern_info['use_case']}")
        print(f"  Example: {pattern_info['example']}")

if __name__ == "__main__":
    print_header("Utility Functions Module")
    print_design_patterns()
