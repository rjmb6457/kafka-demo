import os
import shutil
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
import json
from audit_logger import AuditLogger

EXTRACT_DIR = "/app/extract"
ARCHIVE_DIR = "/app/archive"
RETENTION_DAYS = 1
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-service:9092")
TOPIC = os.getenv("TOPIC", "transactions")

seen_records = {}

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=3,
    acks="all"
)

# heartbeat every 60s
audit = AuditLogger("Producer", interval=60)

def process_file(file_path):
    with open(file_path, "r") as f:
        lines = f.readlines()
        source_count = len(lines)
        audit.set_source_records(source_count, file_path)

        produced_count = 0
        print(f"[START] Processing file: {file_path} with {source_count} records", flush=True)

        for line in lines:
            try:
                record = json.loads(line.strip())
                msg_id = record.get("msg_id")

                if msg_id not in seen_records:
                    seen_records[msg_id] = record
                    print(f"[NEW] Sending record {msg_id}: {record}", flush=True)
                    future = producer.send(TOPIC, record)
                    metadata = future.get(timeout=10)
                    produced_count += 1
                    audit.log("produced", f"Produced msg_id={msg_id} offset={metadata.offset}")

                elif seen_records[msg_id] != record:
                    seen_records[msg_id] = record
                    print(f"[UPDATE] Sending updated record {msg_id}: {record}", flush=True)
                    future = producer.send(TOPIC, record)
                    metadata = future.get(timeout=10)
                    produced_count += 1
                    audit.log("produced", f"Updated msg_id={msg_id} offset={metadata.offset}")

                else:
                    print(f"[DUPLICATE] Skipping record {msg_id}", flush=True)

            except Exception as e:
                print(f"[ERROR] Failed to process line: {line.strip()} | {e}", flush=True)
                audit.log("final_fail", f"Failed to send record: {e}")

    producer.flush()
    # End validation
    if produced_count == source_count:
        print(f"[SUCCESS][Producer] Delivered all {produced_count}/{source_count} records from {file_path}", flush=True)
    else:
        print(f"[ERROR][Producer] Mismatch for {file_path}: SourceRecords={source_count}, Produced={produced_count}", flush=True)

    print(f"[END] Finished sending records from {file_path}", flush=True)

def archive_file(file_path):
    base = os.path.basename(file_path)
    new_name = base + "_done"
    dest = os.path.join(ARCHIVE_DIR, new_name)
    shutil.move(file_path, dest)
    print(f"[ARCHIVE] Moved file to {dest}", flush=True)

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