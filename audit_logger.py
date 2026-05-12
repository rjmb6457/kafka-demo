import time
import threading
from datetime import datetime

class AuditLogger:
    def __init__(self, component_name, interval=60):  # heartbeat every 1 minute
        self.component = component_name
        self.interval = interval
        self.start_time = time.time()
        self.counts = {
            "source_records": 0,
            "produced": 0,
            "consumed": 0,
            "retries": 0,
            "dlq": 0,
            "reprocessed": 0,
            "final_fail": 0,
            "lag": 0
        }
        threading.Thread(target=self._heartbeat, daemon=True).start()

    def log(self, key, msg=""):
        if key in self.counts:
            self.counts[key] += 1
        if msg:
            print(f"[AUDIT][{self.component}] {msg}", flush=True)

    # --- Producer-specific helpers ---
    def set_source_records(self, count, file_path=""):
        self.counts["source_records"] = count
        print(f"[START][{self.component}] File {file_path} contains {count} records", flush=True)

    def finalize_production(self, file_path=""):
        src = self.counts["source_records"]
        prod = self.counts["produced"]
        if prod == src:
            print(f"[SUCCESS][{self.component}] Delivered all {prod} records from {file_path}", flush=True)
        else:
            print(f"[ERROR][{self.component}] Mismatch for {file_path}: "
                  f"SourceRecords={src}, Produced={prod}", flush=True)

    # --- Consumer-specific helpers ---
    def set_lag(self, lag_value):
        self.counts["lag"] = lag_value

    # --- Metrics heartbeat ---
    def report(self):
        elapsed = int(time.time() - self.start_time)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if self.component == "Producer":
            print(f"[METRICS][Producer] {timestamp} | Elapsed={elapsed}s | "
                  f"SourceRecords={self.counts['source_records']} | "
                  f"Produced={self.counts['produced']} | "
                  f"Retries={self.counts['retries']} | "
                  f"FinalFail={self.counts['final_fail']}", flush=True)

        elif self.component == "Consumer":
            print(f"[METRICS][Consumer] {timestamp} | Elapsed={elapsed}s | "
                  f"Consumed={self.counts['consumed']} | "
                  f"Retries={self.counts['retries']} | "
                  f"DLQ={self.counts['dlq']} | "
                  f"Reprocessed={self.counts['reprocessed']} | "
                  f"FinalFail={self.counts['final_fail']} | "
                  f"Lag={self.counts['lag']}", flush=True)

        elif self.component == "Reprocessor":
            print(f"[METRICS][Reprocessor] {timestamp} | Elapsed={elapsed}s | "
                  f"Reprocessed={self.counts['reprocessed']}", flush=True)

    def _heartbeat(self):
        while True:
            time.sleep(self.interval)
            self.report()