import os
import shutil
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
import json

EXTRACT_DIR = "/app/extract"
ARCHIVE_DIR = "/app/archive"
RETENTION_DAYS = 7  # number of retention days for archived files
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-service:9092")
TOPIC = os.getenv("TOPIC", "transactions")

# Track seen records by ID, storing the last payload
seen_records = {}

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=3,
    acks="all"
)

def process_file(file_path):
    print(f"[START] Processing file: {file_path}", flush=True)
    with open(file_path, "r") as f:
        for line in f:
            try:
                record = json.loads(line.strip())
                msg_id = record.get("msg_id")

                if msg_id not in seen_records:
                    # First time we see this ID
                    seen_records[msg_id] = record
                    print(f"[NEW] Sending record {msg_id}: {record}", flush=True)
                    producer.send(TOPIC, record)

                elif seen_records[msg_id] != record:
                    # Same ID but payload changed → UPDATE
                    seen_records[msg_id] = record
                    print(f"[UPDATE] Sending updated record {msg_id}: {record}", flush=True)
                    producer.send(TOPIC, record)

                else:
                    # Same ID, same payload → DUPLICATE
                    print(f"[DUPLICATE] Skipping record {msg_id}", flush=True)

            except Exception as e:
                print(f"[ERROR] Failed to process line: {line.strip()} | {e}", flush=True)

    producer.flush()
    print(f"[END] Finished sending records from {file_path}", flush=True)

def archive_file(file_path):
    base = os.path.basename(file_path)
    new_name = base + "_done"
    dest = os.path.join(ARCHIVE_DIR, new_name)
    shutil.move(file_path, dest)
    print(f"[ARCHIVE] Moved file to {dest}", flush=True)

    # cleanup old files
    cutoff = datetime.now() - timedelta(days=RETENTION_DAYS)
    for f in os.listdir(ARCHIVE_DIR):
        full_path = os.path.join(ARCHIVE_DIR, f)
        if os.path.isfile(full_path):
            mtime = datetime.fromtimestamp(os.path.getmtime(full_path))
            if mtime < cutoff:
                os.remove(full_path)
                print(f"[CLEANUP] Deleted old archive file: {full_path}", flush=True)

def run():
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    print("[INIT] Producer started, watching /app/extract...", flush=True)
    while True:
        for f in os.listdir(EXTRACT_DIR):
            if f.startswith("transactions_") and f.endswith(".jsonl"):
                file_path = os.path.join(EXTRACT_DIR, f)
                process_file(file_path)
                archive_file(file_path)
        time.sleep(5)

if __name__ == "__main__":
    run()