/**
 * Consume and process tasks from an EZRA queue using a pool of async workers.
 *
 * Each worker maintains its own connection and unique identity. Parallel
 * processing is visible in the interleaved, color-coded log output.
 *
 * Scale containers with:
 *   docker compose up --scale worker=3
 *
 * Scale workers per container with:
 *   NUM_WORKERS=20 docker compose up
 *
 * Environment:
 *   EZRA_HOST      default: localhost
 *   EZRA_PORT      default: 42002
 *   EZRA_QUEUE     default: tasks
 *   NUM_WORKERS    parallel workers per container (default: 10)
 *   IDLE_TIMEOUT   seconds with no tasks before a worker exits (default: 3)
 *   SPEED          divide task duration by this; 5 = 5x faster (default: 5)
 *   NACK_MODE      immediate: nack with xdel, task retries right away (default)
 *                  deferred:  skip nack, EZRA reclaims after visibility_timeout seconds
 */

import os from 'os'
import { createClient } from 'redis'

const host        = process.env.EZRA_HOST    || 'localhost'
const port        = parseInt(process.env.EZRA_PORT    || '42002', 10)
const queue       = process.env.EZRA_QUEUE   || 'tasks'
const idleTimeout = parseInt(process.env.IDLE_TIMEOUT || '3',  10)
const speed       = parseFloat(process.env.SPEED      || '5')
const numWorkers  = parseInt(process.env.NUM_WORKERS  || '10', 10)
const nackMode    = process.env.NACK_MODE    || 'immediate'
const GROUP       = 'workers'
const BLOCK_MS    = 1000

const COLORS = [
  '\x1b[36m',  // cyan
  '\x1b[32m',  // green
  '\x1b[33m',  // yellow
  '\x1b[35m',  // magenta
  '\x1b[94m',  // bright blue
  '\x1b[96m',  // bright cyan
  '\x1b[92m',  // bright green
  '\x1b[93m',  // bright yellow
  '\x1b[95m',  // bright magenta
  '\x1b[34m',  // blue
]
const RESET = '\x1b[0m'
const BOLD  = '\x1b[1m'
const DIM   = '\x1b[2m'
const RED   = '\x1b[31m'

function log(color, label, action, detail = '') {
  console.log(`${color}[${label}]${RESET}  ${action.padEnd(5)}  ${detail}`)
}

const sleep = ms => new Promise(r => setTimeout(r, ms))

async function handleTask(task) {
  await sleep((parseFloat(task.duration || 1) / speed) * 1000)
  if (task.will_fail) throw new Error(`task ${task.id} is marked will_fail`)
}

async function runWorker(index) {
  const workerId = `${os.hostname()}-w${String(index).padStart(2, '0')}`
  const label    = `w-${String(index).padStart(2, '0')}`
  const color    = COLORS[(index - 1) % COLORS.length]

  const client = createClient({ socket: { host, port } })
  await client.connect()
  await client.clientSetName(workerId)

  try {
    await client.xGroupCreate(queue, GROUP, '$', { MKSTREAM: true })
  } catch (err) {
    if (!err.message.includes('BUSYGROUP')) throw err
  }

  log(color, label, 'ready')

  let done      = 0
  let nacked    = 0
  let idleSince = null

  while (true) {
    const response = await client.xReadGroup(
      GROUP, workerId,
      [{ key: queue, id: '>' }],
      { COUNT: 1, BLOCK: BLOCK_MS }
    )

    if (!response) {
      if (idleSince === null) idleSince = Date.now()
      if ((Date.now() - idleSince) / 1000 >= idleTimeout) {
        log(color, label, 'exit', `done=${done}  nacked=${nacked}`)
        break
      }
      continue
    }

    idleSince = null

    const { messages } = response[0]
    for (const { id: ezraId, message: fields } of messages) {
      const attempts    = fields.attempts     || '1'
      const maxAttempts = fields.max_attempts || '3'

      let task = {}
      try { task = JSON.parse(fields.payload || '{}') } catch { /* keep empty */ }

      const taskId   = String(task.id ?? '?').padStart(3)
      const duration = task.duration ?? '?'

      log(color, label, 'pop', `id=${taskId}  ${duration}s  ${attempts}/${maxAttempts}`)

      try {
        await handleTask(task)
        await client.xAck(queue, GROUP, ezraId)
        done++
        log(color, label, 'ack', `id=${taskId}`)
      } catch (err) {
        nacked++
        log(color, label, 'nack', `${RED}id=${taskId}  ${attempts}/${maxAttempts}  ${err.message}${RESET}`)
        if (nackMode === 'immediate') {
          await client.xDel(queue, ezraId)
        }
      }
    }
  }

  await client.disconnect()
  return { done, nacked }
}

// --- main ---

const hostname = os.hostname()
process.stdout.write(
  `\n${BOLD}ezra  ${RESET}${DIM}host=${hostname}  workers=${numWorkers}  queue=${queue}  ` +
  `idle_timeout=${idleTimeout}s  nack=${nackMode}${RESET}\n\n`
)

const results = await Promise.all(
  Array.from({ length: numWorkers }, (_, i) => runWorker(i + 1))
)

const totalDone   = results.reduce((s, r) => s + r.done,   0)
const totalNacked = results.reduce((s, r) => s + r.nacked, 0)

process.stdout.write(`\n${BOLD}done${RESET}  workers=${numWorkers}  tasks=${totalDone}  nacked=${totalNacked}\n`)
for (let i = 0; i < results.length; i++) {
  const r       = results[i]
  const color   = COLORS[i % COLORS.length]
  const label   = `w-${String(i + 1).padStart(2, '0')}`
  const nackStr = r.nacked ? `  ${RED}nacked=${r.nacked}${RESET}` : ''
  process.stdout.write(`  ${color}[${label}]${RESET}  done=${r.done}${nackStr}\n`)
}
process.stdout.write('\n')
