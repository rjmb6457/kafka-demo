import datetime
import json
import random
import os
import time

EXTRACT_DIR = "/app/extract"

def generate_file():
    # timestamped filename
    ts = datetime.datetime.now().strftime("%m%d%Y_%H%M%S")
    filename = f"transactions_{ts}.jsonl"
    path = os.path.join(EXTRACT_DIR, filename)

    # create file and write 10 records
    with open(path, "w") as f:
        for i in range(10):
            record = {
                "msg_id": f"txn-{random.randint(100000,999999)}",
                "account_id": f"ACC{random.randint(1000,9999)}",
                "transaction_type": random.choice(["deposit", "withdrawal", "transfer"]),
                "amount": random.randint(100, 5000),
                "location": random.choice(["Makati Branch", "Cebu Branch", "Ortigas Branch"]),
                "timestamp": datetime.datetime.now().isoformat(),
                "retry": 0,
                "reprocess": False
            }
            f.write(json.dumps(record) + "\n")
            print(record, flush=True)   # <-- print each record to logs

    print(f"Generated file: {path}", flush=True)

def run():
    os.makedirs(EXTRACT_DIR, exist_ok=True)
    while True:
        generate_file()
        time.sleep(30)  # new file every 30 seconds

if __name__ == "__main__":
    run()