import os
import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

# --- Kafka Producer Configuration ---
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',        # wait for all replicas to acknowledge
    retries=5,         # retry sending if broker is unavailable
    linger_ms=10       # small batching delay for efficiency
)

EXTRACT_DIR = "./extract"
ARCHIVE_DIR = "./archive"

# --- Counters ---
stats = {"sent": 0, "errors": 0, "files_processed": 0}

# --- Send Function with Error Handling ---
def send_record(record):
    try:
        future = producer.send(record["topic"], record)
        metadata = future.get(timeout=10)  # synchronous send, waits for ack
        stats["sent"] += 1
        print(f"Produced to {metadata.topic}: {record}")
    except KafkaError as e:
        stats["errors"] += 1
        print(f"Error producing record {record}: {e}")

# --- Main Loop: Process files in extract directory ---
for filename in os.listdir(EXTRACT_DIR):
    if filename.endswith(".json"):
        filepath = os.path.join(EXTRACT_DIR, filename)
        try:
            with open(filepath) as f:
                records = json.load(f)
                for rec in records:
                    send_record(rec)
                    time.sleep(0.2)  # simulate staggered ingestion
            stats["files_processed"] += 1

            # Move file to archive after successful processing
            archive_path = os.path.join(ARCHIVE_DIR, filename.replace(".json", "_done.json"))
            os.rename(filepath, archive_path)
            print(f"Archived {filename} -> {archive_path}")

        except Exception as e:
            print(f"Error reading {filename}: {e}")

# --- Finalize ---
producer.flush()
producer.close()
print("Summary:", stats)

