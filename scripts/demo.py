"""Submit a batch of example tasks and watch them drain. Run with the stack up:

    python scripts/demo.py [count]
"""

import json
import random
import sys
import time
import urllib.request

API = "http://localhost:8000"


def call(method: str, path: str, body: dict | None = None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(API + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as resp:
        return json.load(resp)


def main(count: int):
    for i in range(count):
        kind = random.choice(["add", "sleep", "flaky"])
        payload = {"add": {"a": i, "b": i}, "sleep": {"seconds": random.uniform(0.2, 2)},
                   "flaky": {"fail_rate": 0.5}}[kind]
        call("POST", "/tasks", {"name": kind, "payload": payload,
                                "queue": random.choice(["high", "default", "low"])})
    print(f"submitted {count} tasks")

    while True:
        s = call("GET", "/stats")
        t = s["tasks"]
        print(f"queued={t['queued']} running={t['running']} retrying={t['retrying']} "
              f"succeeded={t['succeeded']} dead={t['dead']} delayed={s['broker']['delayed']}")
        if t["queued"] + t["running"] + t["retrying"] == 0:
            break
        time.sleep(1)


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 50)
