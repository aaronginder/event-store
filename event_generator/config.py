from datetime import datetime, timedelta

# Event generation configuration
EVENT_CONFIG = {
    'product_view': {
        'num_events': 5000000,
    },
    'order': {
        'num_events': 1000000,
    },
    'account': {
        'num_events': 1000000,
    }
}

# Time range for event generation
START_DATE = datetime.now() - timedelta(days=30)
END_DATE = datetime.now()

# Output configuration
OUTPUT_DIR = './output'
PARTITION_BY = ['event_category']  # Partition the Parquet files by event category