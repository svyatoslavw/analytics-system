import { EventProcessor } from "@/event-processor"
import { Scheduler } from "@/scheduler"
import * as dotenv from "dotenv"
import os from "os"

dotenv.config()

async function start() {
  const scheduler = new Scheduler()
  const eventProcessor = new EventProcessor(scheduler)

  await eventProcessor.initialize()

  console.log(`Worker ${os.hostname()} started and connected to Redis at ${process.env.REDIS_URL}`)
}

start().catch((error) => {
  console.error("FATAL ERROR IN WORKER:", error)
  process.exit(1)
})
