import pandas as pd
import random
from datetime import datetime, timedelta
from faker import Faker
from utils.hash_utils import generate_entity_id

fake = Faker()

def generate_product_view_events(num_events, start_date, end_date):
    """Generate sample product view events.
    
    Args:
        num_events: Number of events to generate
        start_date: Start date for event timestamps
        end_date: End date for event timestamps
        
    Returns:
        DataFrame containing product view events
    """
    events = []
    
    # Convert start_date and end_date to datetime objects if they are strings
    if isinstance(start_date, str):
        start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
    if isinstance(end_date, str):
        end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
    
    # Calculate the time range in seconds
    time_range = (end_date - start_date).total_seconds()
    
    product_ids = [generate_entity_id(f"product_{i}") for i in range(1, 101)]
    user_ids = [generate_entity_id(f"user_{i}") for i in range(1, 501)]
    
    for _ in range(num_events):
        # Generate a random timestamp within the range
        random_seconds = random.uniform(0, time_range)
        event_timestamp = start_date + timedelta(seconds=random_seconds)
        
        # Generate event data
        event = {
            'event_name': 'product_view',
            'event_timestamp': event_timestamp,
            'event_category': 'product',
            'event_value': {
                'product_id': random.choice(product_ids),
                'category': random.choice(['electronics', 'clothing', 'home', 'books', 'toys']),
                'view_duration_seconds': round(random.uniform(5, 300), 2),
                'source_page': random.choice(['home', 'search', 'category', 'recommendation'])
            },
            'entity_id': random.choice(user_ids)
        }
        events.append(event)
    
    # Convert event_value dict to string representation for Parquet compatibility
    df = pd.DataFrame(events)
    df['event_value'] = df['event_value'].apply(lambda x: str(x))
    
    return df