import { createClient } from "@clickhouse/client"
import Redis from "ioredis"
import { config } from "./config"
import { logger } from "./logger"
import { Scheduler } from "./scheduler"
import { CreateEventType } from "./schemes"
import { RedisXReadGroupResponse } from "./types"

const redis = new Redis(process.env.REDIS_URL)
const clickhouse = createClient({ url: process.env.CLICKHOUSE_URL })

const metrics = {
  processed: 0,
  errors: 0,
  lastProcessed: new Date(),
  isHealthy: true
}

class EventProcessor {
  private isProcessing = false
  private scheduler = new Scheduler()
  private pendingAck: Map<string, string> = new Map()

  async initialize() {
    try {
      await redis.xgroup("CREATE", "events_stream", "worker_group", "$", "MKSTREAM")
      logger.info("Consumer group 'worker_group' created")
    } catch (error) {
      logger.warn("Consumer group 'worker_group' already exists")
    }
  }

  async processEvents() {
    if (this.isProcessing) {
      logger.warn("Event processing is already running")
      return
    }

    this.isProcessing = true
    metrics.lastProcessed = new Date()

    try {
      const result = await redis.xreadgroup(
        "GROUP",
        config.consumerGroup,
        config.consumerName,
        "COUNT",
        config.batchSize,
        "BLOCK",
        config.pollInterval,
        "STREAMS",
        config.streamName,
        ">"
      )

      if (!result || !result.length) {
        this.isProcessing = false
        return
      }

      const entries = result as RedisXReadGroupResponse
      const batchPromises = []
      const processedIds: string[] = []

      for (const [stream, messages] of entries) {
        for (const [id, fields] of messages) {
          try {
            const event = JSON.parse(fields[1])
            const insertPromise = this.insertToClickhouse(event)
              .then(() => this.acknowledgeMessage(id))
              .catch(async (error) => {
                logger.error(`Failed to process message ${id}:`, error)
                await this.handleFailedMessage(id)
              })

            batchPromises.push(insertPromise)
            processedIds.push(id)
          } catch (error) {
            logger.error(`Failed to parse message ${id}:`, error)
            await this.acknowledgeMessage(id)
          }
        }
      }

      await Promise.allSettled(batchPromises)
      metrics.processed += processedIds.length
      logger.debug(`Processed ${processedIds.length} messages`)
    } catch (error) {
      metrics.errors += 1
      logger.error("Error processing events:", error)

      const backoffTime = Math.min(1000 * 2 ** metrics.errors, 10000)
      await new Promise((resolve) => setTimeout(resolve, backoffTime))
    } finally {
      this.isProcessing = false
    }
  }

  private async insertToClickhouse(event: CreateEventType) {
    await clickhouse.insert({
      format: "JSONEachRow",
      table: "events",
      values: [
        {
          event_name: event.eventName,
          user_id: event.userId,
          timestamp: event.timestamp,
          metadata: JSON.stringify(event.metadata || {})
        }
      ]
    })
  }

  private async acknowledgeMessage(messageId: string) {
    try {
      await redis.xack(config.streamName, config.consumerGroup, messageId)
      this.pendingAck.delete(messageId)
    } catch (error) {
      logger.error(`Failed to acknowledge message ${messageId}:`, error)
      this.pendingAck.set(messageId, new Date().toISOString())
    }
  }

  private async handleFailedMessage(messageId: string) {
    const retryCount = await this.getRatryCount(messageId)
    if (retryCount < config.maxRetries) {
      logger.info(`Retrying message ${messageId}. Attempt ${retryCount + 1}`)
      await this.retryMessage(messageId)
    } else {
      logger.warn(`Message ${messageId} reached max retries. Acknowledging and skipping.`)
      await this.acknowledgeMessage(messageId)
    }
  }

  private async getRatryCount(messageId: string): Promise<number> {
    return 0
  }
  private async retryMessage(messageId: string) {
    return
  }
}
