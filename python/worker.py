"""
Consume and process tasks from an EZRA queue using a pool of worker threads.

Each thread maintains its own connection and unique identity. Parallel
processing is visible in the interleaved, color-coded log output.

Scale containers with:
  docker compose up --scale worker=3

Scale threads per container with:
  NUM_WORKERS=20 docker compose up

Environment:
  EZRA_HOST      default: localhost
  EZRA_PORT      default: 42002
  EZRA_QUEUE     default: tasks
  NUM_WORKERS    parallel worker threads per container (default: 10)
  IDLE_TIMEOUT   seconds with no tasks before a worker exits (default: 30)
  NACK_MODE      immediate: nack with xdel, task retries right away (default)
                 deferred:  skip nack, EZRA reclaims after visibility_timeout seconds
"""

import json
import os
import socket
import time
import threading

import redis


QUEUE       = os.getenv("EZRA_QUEUE", "tasks")
GROUP       = "workers"
BLOCK_MS    = 1_000
NACK_MODE   = os.getenv("NACK_MODE", "immediate")
NUM_WORKERS = int(os.getenv("NUM_WORKERS", "10"))
SPEED       = float(os.getenv("SPEED", "1.0"))

_COLORS = [
    "\033[36m",  # cyan
    "\033[32m",  # green
    "\033[33m",  # yellow
    "\033[35m",  # magenta
    "\033[94m",  # bright blue
    "\033[96m",  # bright cyan
    "\033[92m",  # bright green
    "\033[93m",  # bright yellow
    "\033[95m",  # bright magenta
    "\033[34m",  # blue
]
_RESET = "\033[0m"
_BOLD  = "\033[1m"
_DIM   = "\033[2m"
_RED   = "\033[31m"

_print_lock = threading.Lock()


def _log(color: str, label: str, action: str, detail: str = "") -> None:
    with _print_lock:
        print(f"{color}[{label}]{_RESET}  {action:<5}  {detail}", flush=True)


def _connect(worker_id: str) -> redis.Redis:
    host = os.getenv("EZRA_HOST", "localhost")
    port = int(os.getenv("EZRA_PORT", "42002"))
    return redis.Redis(
        host=host, port=port,
        decode_responses=True, protocol=3,
        client_name=worker_id,
    )


def _ensure_group(client: redis.Redis) -> None:
    try:
        client.xgroup_create(QUEUE, GROUP, id="$", mkstream=True)
    except redis.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def _process(task: dict) -> None:
    time.sleep(float(task.get("duration", 1.0)) / SPEED)
    if task.get("will_fail"):
        raise RuntimeError(f"task {task.get('id')} is marked will_fail")


def _run(index: int, idle_timeout: int, results: dict) -> None:
    hostname  = socket.gethostname()
    worker_id = f"{hostname}-w{index:02d}"
    label     = f"w-{index:02d}"
    color     = _COLORS[(index - 1) % len(_COLORS)]

    client = _connect(worker_id)
    _ensure_group(client)
    _log(color, label, "ready")

    done:   int         = 0
    nacked: int         = 0
    idle_since: float | None = None

    while True:
        response = client.xreadgroup(
            GROUP, worker_id, streams={QUEUE: ">"}, count=1, block=BLOCK_MS
        )

        if not response:
            if idle_since is None:
                idle_since = time.monotonic()
            elif time.monotonic() - idle_since >= idle_timeout:
                _log(color, label, "exit", f"done={done}  nacked={nacked}")
                break
            continue

        idle_since = None

        for _stream, (messages,) in response.items():
            for ezra_id, fields in messages:
                try:
                    task = json.loads(fields.get("payload", "{}"))
                except json.JSONDecodeError:
                    task = {}

                task_id      = task.get("id", "?")
                duration     = task.get("duration", "?")
                attempts     = fields.get("attempts", "1")
                max_attempts = fields.get("max_attempts", "3")

                _log(color, label, "pop",
                    f"id={str(task_id):>3}  {duration}s  {attempts}/{max_attempts}")

                try:
                    _process(task)
                    client.xack(QUEUE, GROUP, ezra_id)
                    done += 1
                    _log(color, label, "ack", f"id={str(task_id):>3}")

                except Exception as exc:
                    nacked += 1
                    _log(color, label, "nack",
                        f"{_RED}id={str(task_id):>3}  {attempts}/{max_attempts}  {exc}{_RESET}")
                    if NACK_MODE == "immediate":
                        client.xdel(QUEUE, ezra_id)

    results[index] = {"done": done, "nacked": nacked}
    client.close()


def main() -> None:
    idle_timeout = int(os.getenv("IDLE_TIMEOUT", "30"))
    hostname     = socket.gethostname()

    print(
        f"\n{_BOLD}ezra  {_RESET}{_DIM}host={hostname}  "
        f"workers={NUM_WORKERS}  queue={QUEUE}  "
        f"idle_timeout={idle_timeout}s  nack={NACK_MODE}{_RESET}\n",
        flush=True,
    )

    results: dict[int, dict] = {}
    threads = [
        threading.Thread(target=_run, args=(i, idle_timeout, results), daemon=True)
        for i in range(1, NUM_WORKERS + 1)
    ]

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    total_done   = sum(r["done"]   for r in results.values())
    total_nacked = sum(r["nacked"] for r in results.values())

    print(f"\n{_BOLD}done{_RESET}  workers={NUM_WORKERS}  tasks={total_done}  nacked={total_nacked}")
    for i in sorted(results):
        r      = results[i]
        color  = _COLORS[(i - 1) % len(_COLORS)]
        label  = f"w-{i:02d}"
        nack_s = f"  {_RED}nacked={r['nacked']}{_RESET}" if r["nacked"] else ""
        print(f"  {color}[{label}]{_RESET}  done={r['done']}{nack_s}")
    print(flush=True)


if __name__ == "__main__":
    main()
