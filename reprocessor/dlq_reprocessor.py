from kafka import KafkaConsumer, KafkaProducer
import json
from audit_logger import AuditLogger

dlq_consumer = KafkaConsumer(
    "transactions-dlq",
    bootstrap_servers="kafka-service:9092",
    group_id="dlq-reprocessor-group",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers="kafka-service:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

audit = AuditLogger("Reprocessor")

try:
    for message in dlq_consumer:
        record = message.value
        record["reprocess"] = True
        producer.send("transactions", record)
        dlq_consumer.commit()
        print(f"[REPROCESS] {record['msg_id']} | {record['transaction_type']} | ₱{record['amount']} | {record['location']}", flush=True)
        audit.log("reprocessed", f"Reprocessed msg_id={record['msg_id']} from DLQ")
except KeyboardInterrupt:
    print("\nDLQ Reprocessor stopped by user.")
finally:
    audit.report()
    producer.flush()
    producer.close()
    dlq_consumer.close()