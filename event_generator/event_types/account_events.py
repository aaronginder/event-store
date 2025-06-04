import pandas as pd
import random
from datetime import datetime, timedelta
from faker import Faker
from utils.hash_utils import generate_entity_id

fake = Faker()

def generate_account_events(num_events, start_date, end_date):
    """Generate sample account events.
    
    Args:
        num_events: Number of events to generate
        start_date: Start date for event timestamps
        end_date: End date for event timestamps
        
    Returns:
        DataFrame containing account events
    """
    events = []
    
    # Convert start_date and end_date to datetime objects if they are strings
    if isinstance(start_date, str):
        start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
    if isinstance(end_date, str):
        end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
    
    # Calculate the time range in seconds
    time_range = (end_date - start_date).total_seconds()
    
    event_names = [
        'account_created', 
        'account_updated', 
        'password_changed',
        'login_success', 
        'login_failed', 
        'logout'
    ]
    
    for _ in range(num_events):
        # Generate a random timestamp within the range
        random_seconds = random.uniform(0, time_range)
        event_timestamp = start_date + timedelta(seconds=random_seconds)
        
        # Create a unique user ID
        user_id = generate_entity_id()
        
        # Choose an event name
        event_name = random.choice(event_names)
        
        # Generate event data based on the event name
        if event_name == 'account_created':
            event_value = {
                'email': fake.email(),
                'name': fake.name(),
                'registration_source': random.choice(['web', 'mobile_app', 'social_media']),
                'marketing_opt_in': random.choice([True, False])
            }
        elif event_name == 'account_updated':
            event_value = {
                'field_updated': random.choice(['name', 'email', 'address', 'phone']),
                'previous_value': fake.word(),
                'new_value': fake.word()
            }
        elif event_name == 'password_changed':
            event_value = {
                'source': random.choice(['user_initiated', 'reset_flow']),
                'password_strength': random.choice(['weak', 'medium', 'strong'])
            }
        elif event_name in ['login_success', 'login_failed']:
            event_value = {
                'device_type': random.choice(['desktop', 'mobile', 'tablet']),
                'browser': random.choice(['chrome', 'firefox', 'safari', 'edge']),
                'ip_address': fake.ipv4(),
                'location': fake.city() + ', ' + fake.country()
            }
            if event_name == 'login_failed':
                event_value['reason'] = random.choice(['incorrect_password', 'account_locked', 'suspicious_location'])
        else:  # logout
            event_value = {
                'session_duration_minutes': random.randint(1, 120),
                'logout_type': random.choice(['user_initiated', 'session_timeout', 'forced_by_system'])
            }
        
        # Generate event data
        event = {
            'event_name': event_name,
            'event_timestamp': event_timestamp,
            'event_category': 'account',
            'event_value': event_value,
            'entity_id': user_id
        }
        events.append(event)
    
    # Convert event_value dict to string representation for Parquet compatibility
    df = pd.DataFrame(events)
    df['event_value'] = df['event_value'].apply(lambda x: str(x))
    
    return df