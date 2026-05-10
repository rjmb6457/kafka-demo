from kafka import KafkaConsumer, KafkaProducer
import json

dlq_consumer = KafkaConsumer(
    "transactions-dlq",
    bootstrap_servers="kafka-service:9092",
    group_id="dlq-reprocessor-group",   # required for commits
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers="kafka-service:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

reprocess_count = 0

try:
    for message in dlq_consumer:
        record = message.value
        record["reprocess"] = True  # mark as reprocessed
        producer.send("transactions", record)
        dlq_consumer.commit()
        reprocess_count += 1
        print(f"[REPROCESS] {record['msg_id']} | {record['transaction_type']} | ₱{record['amount']} | {record['location']}")
except KeyboardInterrupt:
    print("\nDLQ Reprocessor stopped by user.")
finally:
    print("=== DLQ Reprocessor Summary ===")
    print(f"Reprocessed: {reprocess_count}")
    producer.flush()
    producer.close()
    dlq_consumer.close()