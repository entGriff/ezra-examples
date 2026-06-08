"""
Push tasks into an EZRA queue.

Each task has:
  id        - sequential number
  duration  - simulated work time in seconds (0.1-10, log-uniform)
  will_fail - if true, the worker raises, triggering the retry/dead-letter path

About 3% of tasks (id % 33 == 0) are marked will_fail so dead-letter behavior
shows up deterministically without having to wait for random failures.

Environment:
  EZRA_HOST    default: localhost
  EZRA_PORT    default: 42002
  EZRA_QUEUE   default: tasks
  TASK_COUNT   default: 50
"""

import json
import math
import os
import random

import redis


QUEUE = os.getenv("EZRA_QUEUE", "tasks")


def task_duration(seed: int) -> float:
    rng = random.Random(seed)
    return round(math.exp(rng.uniform(math.log(0.1), math.log(10.0))), 2)


def main() -> None:
    host = os.getenv("EZRA_HOST", "localhost")
    port = int(os.getenv("EZRA_PORT", "42002"))
    count = int(os.getenv("TASK_COUNT", "50"))

    client = redis.Redis(host=host, port=port, decode_responses=True, protocol=3)

    will_fail_total = 0
    print(f"[producer] pushing {count} tasks to {host}:{port}/{QUEUE}")

    for i in range(1, count + 1):
        will_fail = (i % 33 == 0)
        if will_fail:
            will_fail_total += 1

        client.xadd(QUEUE, {"payload": json.dumps({
            "id":        i,
            "duration":  task_duration(i),
            "will_fail": will_fail,
        })})

        if i % 10 == 0:
            print(f"[producer] {i}/{count}")

    print(
        f"[producer] done  total={count}  "
        f"will_fail={will_fail_total} (-> dead-letter after {3} attempts)"
    )


if __name__ == "__main__":
    main()
