import time, threading
from datetime import datetime

class AuditLogger:
    def __init__(self, component_name, interval=30):
        self.component = component_name
        self.interval = interval
        self.start_time = time.time()
        self.counts = {
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

    def set_lag(self, lag_value):
        self.counts["lag"] = lag_value

    def report(self):
        elapsed = int(time.time() - self.start_time)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[METRICS][{self.component}] {timestamp} | Elapsed={elapsed}s | "
              f"Produced={self.counts['produced']} | "
              f"Consumed={self.counts['consumed']} | "
              f"Retries={self.counts['retries']} | "
              f"DLQ={self.counts['dlq']} | "
              f"Reprocessed={self.counts['reprocessed']} | "
              f"FinalFail={self.counts['final_fail']} | "
              f"Lag={self.counts['lag']}", flush=True)

    def _heartbeat(self):
        while True:
            time.sleep(self.interval)
            self.report()