/**
 * Consume and process tasks from an EZRA queue.
 *
 * Scale with:
 *   docker compose up --scale worker=3
 *
 * Environment:
 *   EZRA_HOST      default: localhost
 *   EZRA_PORT      default: 42002
 *   EZRA_QUEUE     default: tasks
 *   IDLE_TIMEOUT   seconds with no tasks before exiting (default: 30)
 *   NACK_MODE      immediate: nack with xdel, task retries right away (default)
 *                  deferred:  skip nack, EZRA reclaims after visibility_timeout seconds
 */

import os from 'os'
import { createClient } from 'redis'

const host        = process.env.EZRA_HOST    || 'localhost'
const port        = parseInt(process.env.EZRA_PORT    || '42002', 10)
const queue       = process.env.EZRA_QUEUE   || 'tasks'
const idleTimeout = parseInt(process.env.IDLE_TIMEOUT || '30', 10)
const nackMode    = process.env.NACK_MODE    || 'immediate'
const GROUP       = 'workers'
const BLOCK_MS    = 2000

const workerId = os.hostname()

const sleep = ms => new Promise(r => setTimeout(r, ms))

async function handleTask(task) {
  await sleep(parseFloat(task.duration || 1) * 1000)
  if (task.will_fail) throw new Error(`task ${task.id} is marked will_fail`)
}

const client = createClient({ socket: { host, port } })
await client.connect()

// Name this connection - useful for monitoring and debugging.
await client.clientSetName(workerId)

try {
  await client.xGroupCreate(queue, GROUP, '$', { MKSTREAM: true })
} catch (err) {
  if (!err.message.includes('BUSYGROUP')) throw err
}

console.log(
  `[${workerId}] ready  queue=${queue}  idle_timeout=${idleTimeout}s  nack=${nackMode}`
)

let idleSince = null
let done = 0
let nacked = 0

while (true) {
  const response = await client.xReadGroup(
    GROUP, workerId,
    [{ key: queue, id: '>' }],
    { COUNT: 1, BLOCK: BLOCK_MS }
  )

  if (!response) {
    if (idleSince === null) idleSince = Date.now()
    if ((Date.now() - idleSince) / 1000 >= idleTimeout) {
      console.log(`[${workerId}] idle ${idleTimeout}s - done=${done} nacked=${nacked}`)
      break
    }
    continue
  }

  idleSince = null

  const { messages } = response[0]
  for (const { id: taskId, message: fields } of messages) {
    const attempts    = fields.attempts     || '1'
    const maxAttempts = fields.max_attempts || '3'

    let task = {}
    try { task = JSON.parse(fields.payload || '{}') } catch { /* keep empty */ }

    console.log(
      `[${workerId}] pop   id=${String(task.id).padStart(3)}  ` +
      `duration=${task.duration}s  attempt=${attempts}/${maxAttempts}`
    )

    try {
      await handleTask(task)
      await client.xAck(queue, GROUP, taskId)
      done++
      console.log(`[${workerId}] ack   id=${String(task.id).padStart(3)}  task_id=${taskId}`)
    } catch (err) {
      nacked++
      console.error(
        `[${workerId}] nack  id=${String(task.id).padStart(3)}  ` +
        `mode=${nackMode}  attempt=${attempts}/${maxAttempts}  err=${err.message}`
      )

      if (nackMode === 'immediate') {
        // Return the task right away. xdel is a standard Redis command
        // that works with any SDK out of the box.
        await client.xDel(queue, taskId)
        // await client.sendCommand(['XNACK', queue, GROUP, taskId])  // explicit nack, same effect
      }
      // Deferred: EZRA reclaims this task after visibility_timeout seconds.
    }
  }
}

await client.disconnect()
