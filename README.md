# EZRA usage examples

Runnable examples that show EZRA in action. Each directory starts a full stack with Docker Compose: an EZRA server, a producer that pushes tasks, and one or more workers that process them.

- [python/](python/) — redis-py ≥5.0, RESP3
- [node/](node/) — node-redis v4, RESP3

## Quick start

**Prerequisite:** Docker with Compose v2 (`docker compose version`).

```bash
# pick an example
cd python   # or: cd node

# start EZRA, push tasks, and run a worker - all in one
docker compose up --build
```

The producer runs once and exits. The worker processes tasks as they arrive, then idles out after 30 s with no new work.

To push another batch without restarting everything:

```bash
docker compose run --rm producer
```

## Scale workers

```bash
docker compose up --build --scale worker=3
```

Run the producer again. Three workers compete for tasks - each task goes to exactly one worker. The hostname prefix in the logs shows who processed what.

## What the demo does

The producer pushes 50 tasks. Each task has a simulated duration (0.1-10 s, log-uniform) so most finish quickly but a few take longer. About 3% of tasks are marked `will_fail`, which causes the worker to nack them. After `max_attempts` (default 3) the task lands in the dead-letter queue.

## Environment variables

| variable | default | effect |
|---|---|---|
| `TASK_COUNT` | 50 | number of tasks the producer pushes |
| `NACK_MODE` | `immediate` | `immediate` - xdel, task retries right away; `deferred` - skip nack, EZRA reclaims after `visibility_timeout` seconds |
| `IDLE_TIMEOUT` | 30 | seconds a worker waits with no tasks before exiting |
| `EZRA_QUEUE` | `tasks` | queue name |

Pass them inline or set in a `.env` file before starting:

```bash
TASK_COUNT=200 docker compose up --build
```

## Links

- [EZRA repository](https://github.com/entGriff/ezra)
- [Full usage reference](https://github.com/entGriff/ezra/blob/main/docs/usage.md)
