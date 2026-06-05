"""
Consume and process tasks from an EZRA queue.

Scale with:
  docker compose up --scale worker=3

Environment:
  EZRA_HOST      default: localhost
  EZRA_PORT      default: 42002
  EZRA_QUEUE     default: tasks
  IDLE_TIMEOUT   seconds with no tasks before exiting (default: 30)
  NACK_MODE      immediate: nack with xdel, task retries right away (default)
                 deferred:  skip nack, EZRA reclaims after visibility_timeout seconds
"""

import json
import os
import socket
import sys
import time

import redis


QUEUE     = os.getenv("EZRA_QUEUE", "tasks")
GROUP     = "workers"
BLOCK_MS  = 2_000
NACK_MODE = os.getenv("NACK_MODE", "immediate")


def connect(worker_id: str) -> redis.Redis:
    host = os.getenv("EZRA_HOST", "localhost")
    port = int(os.getenv("EZRA_PORT", "42002"))
    # client_name sends CLIENT SETNAME on connect - useful for monitoring and debugging.
    return redis.Redis(host=host, port=port, decode_responses=True, protocol=3, client_name=worker_id)


def ensure_group(client: redis.Redis) -> None:
    try:
        client.xgroup_create(QUEUE, GROUP, id="$", mkstream=True)
    except redis.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def process(task: dict) -> None:
    time.sleep(float(task.get("duration", 1.0)))
    if task.get("will_fail"):
        raise RuntimeError(f"task {task.get('id')} is marked will_fail")


def main() -> None:
    idle_timeout = int(os.getenv("IDLE_TIMEOUT", "30"))
    worker_id    = socket.gethostname()

    client = connect(worker_id)
    ensure_group(client)

    print(
        f"[{worker_id}] ready  queue={QUEUE}  "
        f"idle_timeout={idle_timeout}s  nack={NACK_MODE}",
        flush=True,
    )

    idle_since: float | None = None
    done = 0
    nacked = 0

    while True:
        response = client.xreadgroup(
            GROUP, worker_id, streams={QUEUE: ">"}, count=1, block=BLOCK_MS
        )

        if not response:
            if idle_since is None:
                idle_since = time.monotonic()
            elif time.monotonic() - idle_since >= idle_timeout:
                print(
                    f"[{worker_id}] idle {idle_timeout}s - done={done} nacked={nacked}",
                    flush=True,
                )
                break
            continue

        idle_since = None

        for _stream, (messages,) in response.items():
            for ezra_id, fields in messages:
                try:
                    task = json.loads(fields.get("payload", "{}"))
                except json.JSONDecodeError:
                    task = {}

                attempts     = fields.get("attempts", "1")
                max_attempts = fields.get("max_attempts", "3")

                print(
                    f"[{worker_id}] pop   id={task.get('id'):>3}  "
                    f"duration={task.get('duration', '?')}s  "
                    f"attempt={attempts}/{max_attempts}",
                    flush=True,
                )

                try:
                    process(task)
                    client.xack(QUEUE, GROUP, ezra_id)
                    done += 1
                    print(
                        f"[{worker_id}] ack   id={task.get('id'):>3}  ezra_id={ezra_id}",
                        flush=True,
                    )

                except Exception as exc:
                    nacked += 1
                    print(
                        f"[{worker_id}] nack  id={task.get('id'):>3}  "
                        f"mode={NACK_MODE}  attempt={attempts}/{max_attempts}  err={exc}",
                        file=sys.stderr,
                        flush=True,
                    )

                    if NACK_MODE == "immediate":
                        # Return the task right away. xdel is a standard Redis command
                        # that works with any SDK out of the box.
                        client.xdel(QUEUE, ezra_id)
                        # client.execute_command("XNACK", QUEUE, GROUP, ezra_id)  # explicit nack, same effect
                    else:
                        # Deferred: EZRA reclaims this task after visibility_timeout seconds.
                        pass


if __name__ == "__main__":
    main()
