# Databricks notebook source
%pip install azure-eventhub

from azure.eventhub import EventHubConsumerClient
import json
import time
import uuid
import threading

# Configuration
connection_string = ""  # Replace with actual connection string
eventhub_name = "user-activities"
consumer_group = "$Default"
streaming_dir = "/mnt/temp/event-stream-input"
MAX_RUNTIME_SECONDS = 100  

start_time = time.time()
should_stop = False

def on_event(partition_context, event):
global should_stop
if time.time() - start_time >= MAX_RUNTIME_SECONDS:
    should_stop = True
    client.close()
    return

try:
    event_data = event.body_as_str()
    file_path = f"{streaming_dir}/events_{int(time.time())}_{uuid.uuid4().hex[:8]}.json"
    dbutils.fs.put(file_path, event_data, overwrite=True)
    partition_context.update_checkpoint(event)
except Exception as e:
    print(f"Error processing event: {e}")

def force_exit_timer():
time.sleep(MAX_RUNTIME_SECONDS + 30)
if not should_stop:
    print("FORCE EXIT: Timer triggered emergency exit")
    client.close()

timer_thread = threading.Thread(target=force_exit_timer, daemon=True)
timer_thread.start()

client = EventHubConsumerClient.from_connection_string(
conn_str=connection_string,
consumer_group=consumer_group,
eventhub_name=eventhub_name
)

try:
with client:
    while not should_stop:
        client.receive(on_event=on_event, starting_position="-1", max_wait_time=10)
except Exception as e:
print(f"Error in event receiver: {e}")

dbutils.fs.put(f"{streaming_dir}/bridge_complete_{time.time()}.txt", "Bridge completed successfully", overwrite=True)
print("EVENT HUB BRIDGE COMPLETED SUCCESSFULLY")