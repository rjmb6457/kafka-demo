from kafka import KafkaConsumer, KafkaProducer
import json
from audit_logger import AuditLogger

consumer = KafkaConsumer(
    "transactions", "transactions-dlq",
    bootstrap_servers="kafka-service:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    group_id='demo-consumer-group'
)

producer = KafkaProducer(
    bootstrap_servers="kafka-service:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

processed_records = {}
audit = AuditLogger("Consumer")

def process_message(msg):
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

def update_lag():
    for tp in consumer.assignment():
        end_offset = consumer.end_offsets([tp])[tp]
        position = consumer.position(tp)
        lag = end_offset - (position or 0)
        audit.set_lag(lag)

try:
    for message in consumer:
        record = message.value
        msg_id = record["msg_id"]

        if record.get("reprocess", False):
            status = "REPROCESS"
        else:
            status = classify_message(record)

        try:
            if process_message(record):
                print(f"[{status}] {msg_id} | {record['transaction_type']} | ₱{record['amount']} | {record['location']}", flush=True)
                processed_records[msg_id] = record
                audit.log("consumed", f"{status} msg_id={msg_id} offset={message.offset}")
                consumer.commit()
        except Exception:
            record["retry"] = record.get("retry", 0) + 1
            audit.log("retries", f"Retry msg_id={msg_id} attempt={record['retry']}")
            if record["retry"] >= 3:
                record["reprocess"] = False
                producer.send("transactions-dlq", record)
                print(f"[ERROR->DLQ] {msg_id} | {record['transaction_type']} | ₱{record['amount']} | {record['location']}", flush=True)
                audit.log("dlq", f"Sent msg_id={msg_id} to DLQ")
            else:
                producer.send("transactions", record)
                print(f"[RETRY] {msg_id} attempt {record['retry']}", flush=True)

        update_lag()

finally:
    audit.report()
    consumer.close()
    producer.close()
    print("Consumer stopped.")