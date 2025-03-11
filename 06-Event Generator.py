# Databricks notebook source
# E-commerce Event Generator and Sender
# ---------------------------------------
# This notebook generates realistic e-commerce events and sends them to Azure Event Hub

# Import necessary libraries
import json
import random
import time
from datetime import datetime
import uuid

# Install required package
%pip install azure-eventhub

# Import Event Hub libraries
from azure.eventhub import EventHubProducerClient, EventData

# Configure Event Hub connection
# Replace with your actual connection string from Azure Portal
connection_string = ""  # REPLACE THIS!
eventhub_name = "user-activities"

# Define event types for e-commerce activity
EVENT_TYPES = ["product_view", "add_to_cart", "checkout", "purchase", "product_search"]

# Get real product IDs from our database
try:
    print("Fetching real product IDs from database...")
    products_df = spark.sql("SELECT product_id FROM ecommerce_db.olist_products_dataset")
    real_product_ids = [str(row.product_id) for row in products_df.collect()]
    print(f"Successfully loaded {len(real_product_ids)} real product IDs")
    
    # If we somehow got an empty list, use dummy IDs
    if not real_product_ids:
        print("No products found in database, using dummy IDs")
        real_product_ids = [f"DUMMY_PROD_{i}" for i in range(1, 101)]
except Exception as e:
    print(f"Error loading product IDs: {e}")
    print("Using dummy product IDs instead")
    real_product_ids = [f"DUMMY_PROD_{i}" for i in range(1, 101)]

# Function to generate realistic e-commerce events
def generate_ecommerce_event():
    """Generate a realistic e-commerce event using real product IDs"""
    event_type = random.choice(EVENT_TYPES)
    user_id = f"user_{random.randint(1000, 9999)}"
    product_id = random.choice(real_product_ids) if event_type != "product_search" else None
    timestamp = datetime.now().isoformat()
    event_id = str(uuid.uuid4())
    
    # Base event data
    event = {
        "event_id": event_id,
        "event_type": event_type,
        "user_id": user_id,
        "product_id": product_id,
        "timestamp": timestamp,
        "session_id": f"session_{random.randint(10000, 99999)}"
    }
    
    # Add event-specific properties based on event type
    if event_type == "product_view":
        event["view_duration_seconds"] = random.randint(5, 300)
    elif event_type == "add_to_cart":
        event["quantity"] = random.randint(1, 5)
    elif event_type == "checkout":
        event["cart_value_cents"] = random.randint(5000, 50000)  # Store in cents to avoid floats
        event["items_count"] = random.randint(1, 10)
    elif event_type == "purchase":
        event["purchase_value_cents"] = random.randint(5000, 50000)  # Store in cents to avoid floats
        event["payment_method"] = random.choice(["credit_card", "debit_card", "paypal", "gift_card"])
    elif event_type == "product_search":
        event["search_query"] = random.choice(["phone", "laptop", "headphones", "camera", "watch"])
    
    return event

# Function to generate and send events to Event Hub
def generate_and_send_events(num_events=10):
    """Generate and send a specified number of events to Event Hub"""
    # Create a producer client
    producer = EventHubProducerClient.from_connection_string(
        conn_str=connection_string,
        eventhub_name=eventhub_name
    )
    
    # Track events sent successfully
    events_sent = 0
    
    try:
        # Create initial batch
        event_batch = producer.create_batch()
        
        # Generate and add events to the batch
        for i in range(num_events):
            try:
                # Generate event
                event = generate_ecommerce_event()
                
                # Convert to JSON
                event_json = json.dumps(event)
                
                # Create EventData object
                event_data = EventData(event_json.encode('utf-8'))
                
                # Try to add to batch
                event_batch.add(event_data)
                events_sent += 1
                
            except ValueError:
                # Batch is full, send it
                producer.send_batch(event_batch)
                print(f"Sent batch with {events_sent} events")
                
                # Create a new batch and add the current event
                event_batch = producer.create_batch()
                event_batch.add(EventData(json.dumps(event).encode('utf-8')))
                events_sent = 1
                
            except Exception as e:
                print(f"Error processing event {i}: {e}")
        
        # Send any remaining events in the final batch
        if events_sent > 0:
            producer.send_batch(event_batch)
            print(f"Sent final batch with {events_sent} events")
        
        total_events = events_sent
        print(f"Successfully sent {total_events} events to Event Hub '{eventhub_name}'")
        
    except Exception as e:
        print(f"Error sending events to Event Hub: {e}")
    finally:
        # Always close the producer
        producer.close()

# Test the event generator (uncomment to view sample events)
sample_events = [generate_ecommerce_event() for _ in range(3)]
for event in sample_events:
     print(json.dumps(event, indent=2))

# To send events, uncomment the line below and replace the connection string above
generate_and_send_events(100)