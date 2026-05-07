import json
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

# --- Kafka Consumer ---
consumer = KafkaConsumer(
    'order', 'payment', 'location',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='demo-consumer-group'
)

# --- Kafka Producer for DLQ ---
dlq_producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# --- Fault Tolerance Settings ---
MAX_RETRIES = 3

# --- State Tracking ---
seen_records = {}
stats = {"new": 0, "duplicate": 0, "update": 0, "dlq": 0, "errors": 0}

# --- Processing Function with Retries ---
def process_record(record):
    for attempt in range(MAX_RETRIES):
        try:
            # Simulate business logic (replace with real processing)
            if "id" not in record:
                raise ValueError("Missing ID field")
            return True
        except Exception as e:
            print(f"Retry {attempt+1} failed for record {record}: {e}")
            time.sleep(1)
    return False

# --- Main Loop with graceful shutdown ---
count = 0
try:
    for msg in consumer:
        record = msg.value
        rid = record.get("id")
        count += 1

        # Try to process with retries
        if not process_record(record):
            stats["dlq"] += 1
            stats["errors"] += 1
            dlq_msg = {
                "topic": msg.topic,
                "record": record,
                "error": "Processing failed"
            }
            try:
                dlq_producer.send('dlq', dlq_msg)
                print(f"{count} DLQ: {dlq_msg}")
            except KafkaError as ke:
                print(f"Failed to send to DLQ: {ke}")
            continue

        # Deduplication / Update logic
        if rid not in seen_records:
            seen_records[rid] = record
            stats["new"] += 1
            print(f"{count} NEW: {record}")
        elif seen_records[rid] == record:
            stats["duplicate"] += 1
            print(f"{count} DUPLICATE: {record}")
        else:
            seen_records[rid] = record
            stats["update"] += 1
            print(f"{count} UPDATE: {record}")

except KeyboardInterrupt:
    print("\nConsumer stopped by user.")

finally:
    print("Summary:", stats)
    dlq_producer.flush()
    dlq_producer.close()
    consumer.close()

