# Databricks notebook source
# Modified Stream Processor with guaranteed timeout and foreground processing
from pyspark.sql import functions as F
from pyspark.sql.types import *
import time

# Enable schema auto-merging globally
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# Set a maximum runtime
MAX_RUNTIME_SECONDS = 180  # 3 minutes
start_time = time.time()

# Define schema for our events
event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("view_duration_seconds", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("cart_value_cents", IntegerType(), True),
    StructField("items_count", IntegerType(), True),
    StructField("purchase_value_cents", IntegerType(), True),
    StructField("payment_method", StringType(), True),
    StructField("search_query", StringType(), True)
])

# Check if we have files to process
input_dir = "/mnt/temp/event-stream-input"
files = dbutils.fs.ls(input_dir)
print(f"Found {len(files)} files in input directory")
for file in files[:5]:  # Show first 5 files
    print(f"  {file.name} ({file.size} bytes)")

# Create streaming DataFrames
events_stream = (
  spark
    .readStream
    .schema(event_schema)
    .option("maxFilesPerTrigger", 10)  # Process in small batches
    .json(input_dir)
)

# Add processing timestamp
events_with_time = events_stream.withColumn("processing_time", F.current_timestamp())

# Write raw events to Delta table with schema merging
raw_events_query = (
  events_with_time
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/temp/checkpoints/raw_events")
    .option("mergeSchema", "true")
    .start("/mnt/processed/raw_events")
)

# Create tables for querying if needed
print("Creating external Delta tables...")
spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce_db")
spark.sql("""
CREATE TABLE IF NOT EXISTS ecommerce_db.raw_events
USING DELTA
LOCATION '/mnt/processed/raw_events'
""")

# Process for a limited time and report progress
print(f"Started streaming at {time.strftime('%H:%M:%S')} - will run for {MAX_RUNTIME_SECONDS} seconds")

# Loop with progress reports until timeout
processed_data = False
while time.time() - start_time < MAX_RUNTIME_SECONDS:
    # Check query progress
    progress = raw_events_query.lastProgress
    if progress:
        num_input_rows = progress.get("numInputRows", 0)
        if num_input_rows > 0:
            processed_data = True
            print(f"Progress: {num_input_rows} rows processed in this batch!")
        else:
            print("Waiting for data... (no rows in this batch)")
    else:
        print("Waiting for first batch...")
    
    # Wait a bit before checking again
    time.sleep(10)

# Stop the query gracefully
print("Stopping query...")
raw_events_query.stop()

if processed_data:
    print("SUCCESS: Data was processed during the streaming session")
else:
    print("WARNING: No data was processed during the session")

print(f"Stream processor completed after {int(time.time() - start_time)} seconds")
# Create a marker file to indicate completion
dbutils.fs.put(f"{input_dir}/processor_complete_{time.time()}.txt", 
               f"Stream processor completed", 
               overwrite=True)