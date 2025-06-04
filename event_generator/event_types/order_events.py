import pandas as pd
import random
from datetime import datetime, timedelta
from faker import Faker
from utils.hash_utils import generate_entity_id

fake = Faker()

def generate_order_events(num_events, start_date, end_date):
    """Generate sample order events.
    
    Args:
        num_events: Number of events to generate
        start_date: Start date for event timestamps
        end_date: End date for event timestamps
        
    Returns:
        DataFrame containing order events
    """
    events = []
    
    # Convert start_date and end_date to datetime objects if they are strings
    if isinstance(start_date, str):
        start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
    if isinstance(end_date, str):
        end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
    
    # Calculate the time range in seconds
    time_range = (end_date - start_date).total_seconds()
    
    user_ids = [generate_entity_id(f"user_{i}") for i in range(1, 501)]
    product_ids = [generate_entity_id(f"product_{i}") for i in range(1, 101)]
    
    for _ in range(num_events):
        # Generate a random timestamp within the range
        random_seconds = random.uniform(0, time_range)
        event_timestamp = start_date + timedelta(seconds=random_seconds)
        
        # Generate a list of items in the order
        num_items = random.randint(1, 5)
        items = []
        total_amount = 0
        
        for _ in range(num_items):
            price = round(random.uniform(10, 200), 2)
            quantity = random.randint(1, 3)
            item_total = price * quantity
            total_amount += item_total
            
            items.append({
                'product_id': random.choice(product_ids),
                'price': price,
                'quantity': quantity,
                'item_total': item_total
            })
        
        # Generate event data
        event = {
            'event_name': random.choice(['order_placed', 'order_confirmed', 'order_shipped', 'order_delivered']),
            'event_timestamp': event_timestamp,
            'event_category': 'order',
            'event_value': {
                'order_id': generate_entity_id(),
                'items': items,
                'total_amount': round(total_amount, 2),
                'payment_method': random.choice(['credit_card', 'debit_card', 'paypal', 'bank_transfer']),
                'shipping_address': fake.address().replace('\n', ', ')
            },
            'entity_id': random.choice(user_ids)
        }
        events.append(event)
    
    # Convert event_value dict to string representation for Parquet compatibility
    df = pd.DataFrame(events)
    df['event_value'] = df['event_value'].apply(lambda x: str(x))
    
    return df