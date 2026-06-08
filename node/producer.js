/**
 * Push tasks into an EZRA queue.
 *
 * Each task has:
 *   id        - sequential number
 *   duration  - simulated work time in seconds (0.1-10, log-uniform)
 *   will_fail - if true, the worker throws, triggering the retry/dead-letter path
 *
 * About 3% of tasks (id % 33 === 0) are marked will_fail so dead-letter
 * behavior shows up deterministically.
 *
 * Environment:
 *   EZRA_HOST    default: localhost
 *   EZRA_PORT    default: 42002
 *   EZRA_QUEUE   default: tasks
 *   TASK_COUNT   default: 50
 */

import { createClient } from 'redis'

const host  = process.env.EZRA_HOST  || 'localhost'
const port  = parseInt(process.env.EZRA_PORT  || '42002', 10)
const queue = process.env.EZRA_QUEUE || 'tasks'
const count = parseInt(process.env.TASK_COUNT || '50', 10)

// Deterministic log-uniform duration in [0.1, 10] seconds.
// Same seed always produces the same duration, so runs are reproducible.
function taskDuration(seed) {
  let h = (seed * 2654435761) >>> 0
  h = (h ^ (h >>> 16)) >>> 0
  const x = h / 4294967296
  return Math.round(Math.exp(Math.log(0.1) + x * Math.log(100)) * 100) / 100
}

const client = createClient({ socket: { host, port } })
await client.connect()

let willFailTotal = 0
console.log(`[producer] pushing ${count} tasks to ${host}:${port}/${queue}`)

for (let i = 1; i <= count; i++) {
  const will_fail = (i % 33 === 0)
  if (will_fail) willFailTotal++

  const payload = JSON.stringify({ id: i, duration: taskDuration(i), will_fail })
  await client.xAdd(queue, '*', { payload })

  if (i % 10 === 0) console.log(`[producer] ${i}/${count}`)
}

console.log(
  `[producer] done  total=${count}  ` +
  `will_fail=${willFailTotal} (-> dead-letter after 3 attempts)`
)

await client.disconnect()
