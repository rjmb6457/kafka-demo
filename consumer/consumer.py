from kafka import KafkaConsumer, KafkaProducer
import json

# Subscribe to both main and DLQ topics
consumer = KafkaConsumer(
    "transactions", "transactions-dlq",
    bootstrap_servers="kafka-service:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers="kafka-service:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Track processed records by msg_id
processed_records = {}  # msg_id -> last payload

# Counters for reporting
new_count = updated_count = duplicate_count = retry_count = dlq_count = reprocess_count = 0

def process_message(msg):
    # Simulate failure for high-value transactions unless explicitly reprocessed
    if msg["amount"] > 3000 and not msg.get("reprocess", False):
        raise Exception("High-value transaction requires manual review")
    return True

def classify_message(record):
    msg_id = record["msg_id"]
    if msg_id not in processed_records:
        return "NEW"
    elif processed_records[msg_id] == record:
        return "DUPLICATE"
    else:
        return "UPDATED"

try:
    for message in consumer:
        record = message.value
        msg_id = record["msg_id"]

        # Distinguish reprocessed DLQ records
        if record.get("reprocess", False):
            status = "REPROCESS"
        else:
            status = classify_message(record)

        try:
            if process_message(record):
                print(f"[{status}] {msg_id} | {record['transaction_type']} | ₱{record['amount']} | {record['location']}")
                processed_records[msg_id] = record
                if status == "NEW":
                    new_count += 1
                elif status == "UPDATED":
                    updated_count += 1
                elif status == "DUPLICATE":
                    duplicate_count += 1
                elif status == "REPROCESS":
                    reprocess_count += 1
                consumer.commit()
        except Exception:
            # Initialize retry counter safely
            record["retry"] = record.get("retry", 0) + 1
            retry_count += 1
            if record["retry"] >= 3:
                # Send to DLQ with reprocess flag reset
                record["reprocess"] = False
                producer.send("transactions-dlq", record)
                print(f"[ERROR->DLQ] {msg_id} | {record['transaction_type']} | ₱{record['amount']} | {record['location']}")
                dlq_count += 1
            else:
                # Retry by sending back to main topic
                producer.send("transactions", record)
                print(f"[RETRY] {msg_id} attempt {record['retry']}")
finally:
    print("=== Summary Report ===")
    print(f"New: {new_count}")
    print(f"Updated: {updated_count}")
    print(f"Duplicates: {duplicate_count}")
    print(f"Retries: {retry_count}")
    print(f"DLQ: {dlq_count}")
    print(f"Reprocessed: {reprocess_count}")