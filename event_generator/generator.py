import os
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lit, rand, expr
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime, timedelta
import uuid
import hashlib
import json

import config

def ensure_output_dir(directory):
    """Ensure the output directory exists."""
    os.makedirs(directory, exist_ok=True)

def generate_entity_id(seed=None):
    """Generate a hash-based entity ID."""
    if seed:
        return hashlib.md5(str(seed).encode()).hexdigest()
    else:
        return str(uuid.uuid4())

def initialize_spark():
    """Initialize and configure Spark session for better performance."""
    return SparkSession.builder \
        .appName("EventDataGenerator") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", 100) \
        .config("spark.default.parallelism", 100) \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

def generate_product_view_events_spark(spark, num_events, start_date, end_date):
    """Generate product view events directly in Spark."""
    # Register UDFs
    @udf(returnType=StringType())
    def random_product_id():
        return generate_entity_id(f"product_{random.randint(1, 100)}")
    
    @udf(returnType=StringType())
    def random_category():
        return random.choice(['electronics', 'clothing', 'home', 'books', 'toys'])
    
    @udf(returnType=StringType())
    def random_source():
        return random.choice(['home', 'search', 'category', 'recommendation'])
    
    @udf(returnType=StringType())
    def random_user_id():
        return generate_entity_id(f"user_{random.randint(1, 500)}")
    
    @udf(returnType=StringType())
    def create_event_value(product_id, category, source):
        value = {
            'product_id': product_id,
            'category': category,
            'view_duration_seconds': round(random.uniform(5, 300), 2),
            'source_page': source
        }
        return json.dumps(value)
    
    # Create a base DataFrame with the desired number of rows
    base_df = spark.range(0, num_events)
    
    # Calculate time range in seconds
    time_range_seconds = int((end_date - start_date).total_seconds())
    start_timestamp = start_date.timestamp()
    
    # Create and transform the DataFrame
    return base_df.select(
        lit("product_view").alias("event_name"),
        (expr(f"timestamp_seconds({start_timestamp} + rand() * {time_range_seconds})"))
            .alias("event_timestamp"),
        lit("product").alias("event_category"),
        random_product_id().alias("product_id"),
        random_category().alias("category"),
        random_source().alias("source"),
        random_user_id().alias("entity_id")
    ).withColumn(
        "event_value", 
        create_event_value("product_id", "category", "source")
    ).drop("product_id", "category", "source")

def generate_order_events_spark(spark, num_events, start_date, end_date):
    """Generate order events directly in Spark."""
    # Register UDFs
    @udf(returnType=StringType())
    def random_order_event_name():
        return random.choice(['order_placed', 'order_confirmed', 'order_shipped', 'order_delivered'])
    
    @udf(returnType=StringType())
    def random_user_id():
        return generate_entity_id(f"user_{random.randint(1, 500)}")
    
    @udf(returnType=StringType())
    def create_order_value():
        num_items = random.randint(1, 5)
        items = []
        total_amount = 0
        
        for _ in range(num_items):
            price = round(random.uniform(10, 200), 2)
            quantity = random.randint(1, 3)
            item_total = price * quantity
            total_amount += item_total
            
            items.append({
                'product_id': generate_entity_id(f"product_{random.randint(1, 100)}"),
                'price': price,
                'quantity': quantity,
                'item_total': item_total
            })
        
        value = {
            'order_id': generate_entity_id(),
            'items': items,
            'total_amount': round(total_amount, 2),
            'payment_method': random.choice(['credit_card', 'debit_card', 'paypal', 'bank_transfer']),
            'shipping_address': f"{random.randint(100, 999)} Main St, City, State {random.randint(10000, 99999)}"
        }
        return json.dumps(value)
    
    # Create a base DataFrame with the desired number of rows
    base_df = spark.range(0, num_events)
    
    # Calculate time range in seconds
    time_range_seconds = int((end_date - start_date).total_seconds())
    start_timestamp = start_date.timestamp()
    
    # Create and transform the DataFrame
    return base_df.select(
        random_order_event_name().alias("event_name"),
        (expr(f"timestamp_seconds({start_timestamp} + rand() * {time_range_seconds})"))
            .alias("event_timestamp"),
        lit("order").alias("event_category"),
        create_order_value().alias("event_value"),
        random_user_id().alias("entity_id")
    )

def generate_account_events_spark(spark, num_events, start_date, end_date):
    """Generate account events directly in Spark."""
    # Register UDFs
    @udf(returnType=StringType())
    def random_account_event_name():
        return random.choice([
            'account_created', 'account_updated', 'password_changed',
            'login_success', 'login_failed', 'logout'
        ])
    
    @udf(returnType=StringType())
    def random_user_id():
        return generate_entity_id()
    
    @udf(returnType=StringType())
    def create_account_value(event_name):
        if event_name == 'account_created':
            value = {
                'email': f"user{random.randint(1, 10000)}@example.com",
                'name': f"User {random.randint(1, 10000)}",
                'registration_source': random.choice(['web', 'mobile_app', 'social_media']),
                'marketing_opt_in': random.choice([True, False])
            }
        elif event_name == 'account_updated':
            value = {
                'field_updated': random.choice(['name', 'email', 'address', 'phone']),
                'previous_value': f"old_value_{random.randint(1, 1000)}",
                'new_value': f"new_value_{random.randint(1, 1000)}"
            }
        elif event_name == 'password_changed':
            value = {
                'source': random.choice(['user_initiated', 'reset_flow']),
                'password_strength': random.choice(['weak', 'medium', 'strong'])
            }
        elif event_name in ['login_success', 'login_failed']:
            value = {
                'device_type': random.choice(['desktop', 'mobile', 'tablet']),
                'browser': random.choice(['chrome', 'firefox', 'safari', 'edge']),
                'ip_address': f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
                'location': f"{random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'])}, USA"
            }
            if event_name == 'login_failed':
                value['reason'] = random.choice(['incorrect_password', 'account_locked', 'suspicious_location'])
        else:  # logout
            value = {
                'session_duration_minutes': random.randint(1, 120),
                'logout_type': random.choice(['user_initiated', 'session_timeout', 'forced_by_system'])
            }
        return json.dumps(value)
    
    # Create a base DataFrame with the desired number of rows
    base_df = spark.range(0, num_events)
    
    # Calculate time range in seconds
    time_range_seconds = int((end_date - start_date).total_seconds())
    start_timestamp = start_date.timestamp()
    
    # Create event names first
    with_events = base_df.withColumn("event_name", random_account_event_name())
    
    # Create and transform the DataFrame
    return with_events.select(
        "event_name",
        (expr(f"timestamp_seconds({start_timestamp} + rand() * {time_range_seconds})"))
            .alias("event_timestamp"),
        lit("account").alias("event_category"),
        create_account_value("event_name").alias("event_value"),
        random_user_id().alias("entity_id")
    )

def generate_all_events():
    """Generate all event types and save to Parquet files."""
    print("Starting event generation...")
    start_time = datetime.now()
    
    # Ensure output directory exists
    ensure_output_dir(config.OUTPUT_DIR)
    
    # Initialize Spark
    spark = initialize_spark()
    
    # Process in batches
    batch_size = 1000000  # 1 million events per batch
    output_path = os.path.join(config.OUTPUT_DIR, "events")
    
    # Generate events in batches
    total_product_events = config.EVENT_CONFIG['product_view']['num_events']
    total_order_events = config.EVENT_CONFIG['order']['num_events']
    total_account_events = config.EVENT_CONFIG['account']['num_events']
    
    # Generate all events in batches
    for event_type, total_events, generator_func in [
        ("product view", total_product_events, generate_product_view_events_spark),
        ("order", total_order_events, generate_order_events_spark),
        ("account", total_account_events, generate_account_events_spark)
    ]:
        remaining = total_events
        batch_num = 1
        
        while remaining > 0:
            current_batch_size = min(batch_size, remaining)
            print(f"Generating {event_type} events batch {batch_num}: {current_batch_size} events")
            
            events_df = generator_func(spark, current_batch_size, config.START_DATE, config.END_DATE)
            
            # Write mode: overwrite for first batch of first event type, append for all others
            write_mode = "overwrite" if event_type == "product view" and batch_num == 1 else "append"
            
            print(f"Saving batch {batch_num} with mode {write_mode}...")
            events_df.write.partitionBy(config.PARTITION_BY).mode(write_mode).parquet(output_path)
            
            remaining -= current_batch_size
            batch_num += 1
    
    # Stop Spark session
    spark.stop()
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    total_events = total_product_events + total_order_events + total_account_events
    print(f"Event generation completed in {duration:.2f} seconds.")
    print(f"Total {total_events} events saved to {output_path}")

if __name__ == "__main__":
    generate_all_events()