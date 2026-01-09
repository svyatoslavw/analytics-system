import Redis from "ioredis"
import { logger } from "./logger"
import { Scheduler } from "./scheduler"

const redis = new Redis(process.env.REDIS_URL)

class EventProcessor {
  private isProcessing = false
  private scheduler = new Scheduler()
  private pendingAsk: Map<string, string> = new Map()

  async initialize() {
    try {
      await redis.xgroup("CREATE", "events_stream", "worker_group", "$", "MKSTREAM")
      logger.info("Consumer group 'worker_group' created")
    } catch (error) {
      logger.warn("Consumer group 'worker_group' already exists")
    }
  }
}
