import { IScheduler } from "@/scheduler/scheduler.interface"
import { createClient } from "@clickhouse/client"
import * as dotenv from "dotenv"
import Redis from "ioredis"
import { config } from "../common/config"
import { logger } from "../common/logger"
import { CreateEventType, EventType } from "../schemes"
import { RedisXReadGroupResponse } from "../types"
import { IEventProcessor } from "./event-processor.interface"

dotenv.config()

const redis = new Redis(process.env.REDIS_URL)
const clickhouse = createClient({
  url: process.env.CLICKHOUSE_URL,
  username: "default",
  password: "default"
})

const metrics = {
  processed: 0,
  errors: 0,
  lastProcessed: new Date(),
  isHealthy: true
}

export class EventProcessor implements IEventProcessor {
  private isProcessing = false
  private scheduler: IScheduler
  private pendingAck: Map<string, string> = new Map()

  constructor(scheduler: IScheduler) {
    this.scheduler = scheduler

    this.scheduler.scheduleInterval("processEvents", 5_000, this.processEvents.bind(this))

    this.scheduler.scheduleInterval("reprocessDlq", 15_000, this.reprocessDlq.bind(this))
  }

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

    logger.info("Starting event processing cycle")

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

      await this._processMessages(result as RedisXReadGroupResponse)
    } catch (error) {
      metrics.errors += 1
      logger.error("Error processing events:", error)

      const backoffTime = Math.min(1000 * 2 ** metrics.errors, 10000)
      await new Promise((resolve) => setTimeout(resolve, backoffTime))
    } finally {
      this.isProcessing = false
    }
  }

  private async _processMessages(entries: RedisXReadGroupResponse) {
    const tasks: Promise<void>[] = []
    const processedIds: string[] = []

    for (const [_, messages] of entries) {
      for (const [id, fields] of messages) {
        tasks.push(this._processMessage(id, fields))
        processedIds.push(id)
      }
    }

    await Promise.allSettled(tasks)

    metrics.processed += processedIds.length
    logger.debug(`Processed ${processedIds.length} messages`)
  }

  private async _processMessage(messageId: string, fields: string[]) {
    const event: EventType = JSON.parse(this._parseFields(fields).data)

    try {
      const lock = await this._acquireIdempotencyLock(messageId)

      if (!lock) {
        logger.warn(`Duplicate message ${messageId}, skipping`)
        await this._acknowledgeMessage(messageId)
        return
      }

      await this._insertToClickhouse(event)
      await this._acknowledgeMessage(messageId)

      metrics.processed++
    } catch (error) {
      logger.error(`Failed to process ${messageId}`, error)
      await this._handleFailedMessage(messageId, event, error)
    }
  }

  private async _acquireIdempotencyLock(messageId: string): Promise<boolean> {
    const lockKey = `lock:${messageId}`
    const result = await redis.set(lockKey, "1", "EX", 3600, "NX")

    return result === "OK"
  }

  private async _insertToClickhouse(event: CreateEventType) {
    await clickhouse.insert({
      format: "JSONEachRow",
      table: "events",
      values: [
        {
          event_name: event.eventName,
          user_id: event.userId,
          timestamp: new Date(event.timestamp).toISOString().replace("T", " ").replace("Z", ""),
          metadata: event.metadata || {}
        }
      ]
    })
  }

  private async _acknowledgeMessage(messageId: string) {
    try {
      await redis.xack(config.streamName, config.consumerGroup, messageId)
      await redis.xdel(config.streamName, messageId)
      this.pendingAck.delete(messageId)
    } catch (error) {
      logger.error(`Failed to acknowledge message ${messageId}:`, error)
      this.pendingAck.set(messageId, new Date().toISOString())
    }
  }

  private async _handleFailedMessage(messageId: string, event: EventType, error: any) {
    const retryCount = await this._getRetryCount(messageId)

    if (retryCount < config.maxRetries) {
      logger.info(`Retrying ${messageId}, attempt ${retryCount + 1}`)
      return
    }

    logger.warn(`Message ${messageId} moved to DLQ`)
    await this._moveToDlq(messageId, event, error)
    await this._acknowledgeMessage(messageId)
  }

  private async _getRetryCount(messageId: string): Promise<number> {
    const pendingInfo = (await redis.xpending(
      config.streamName,
      config.consumerGroup,
      messageId,
      messageId,
      1
    )) as Array<[string, string, number, number]>

    if (pendingInfo && pendingInfo.length > 0) {
      const [id, consumer, idleTimeMs, deliveryCount] = pendingInfo[0]
      return deliveryCount
    }

    return 0
  }

  private async _moveToDlq(messageId: string, event: EventType, error: any) {
    await redis.xadd(
      config.dlqStreamName,
      "*",
      "original_id",
      messageId,
      "event_data",
      JSON.stringify(event),
      "error",
      error.message,
      "failed_at",
      new Date().toISOString()
    )
  }

  async reprocessDlq(batchSize = 50) {
    try {
      logger.info("Reprocessing DLQ messages")

      await redis
        .xgroup("CREATE", config.dlqStreamName, "dlq_group", "$", "MKSTREAM")
        .catch(() => {})

      const result = await redis.xreadgroup(
        "GROUP",
        "dlq_group",
        `${config.consumerName}_dlq`,
        "COUNT",
        batchSize,
        "BLOCK",
        1000,
        "STREAMS",
        config.dlqStreamName,
        ">"
      )

      if (!result || !result.length) return

      for (const [, messages] of result as RedisXReadGroupResponse) {
        for (const [id, fields] of messages) {
          try {
            const data: Record<string, string> = this._parseFields(fields)
            const event = JSON.parse(data.event_data)

            const lock = await this._acquireIdempotencyLock(`${id}_dlq`)
            if (!lock) {
              logger.warn(`Duplicate DLQ message ${id}, skipping`)
              await redis.xack(config.dlqStreamName, "dlq_group", id)
              continue
            }

            await redis.xadd(config.streamName, "*", "event_data", JSON.stringify(event))
            await redis.xack(config.dlqStreamName, "dlq_group", id)

            logger.info(`DLQ message ${id} restored to main stream`)
          } catch (error) {
            logger.error(`Failed to reprocess DLQ message ${id}`, error)
          }
        }
      }
    } catch (error) {
      logger.error("Error reprocessing DLQ", error)
    }
  }

  private _parseFields(fields: string[]): Record<string, string> {
    const obj: Record<string, string> = {}
    for (let i = 0; i < fields.length; i += 2) {
      obj[fields[i]] = fields[i + 1]
    }
    return obj
  }
}
