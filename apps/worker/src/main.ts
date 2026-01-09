import { createClient } from "@clickhouse/client"
import * as dotenv from "dotenv"
import Redis from "ioredis"
import os from "os"

dotenv.config()

type StreamName = string
type MessageId = string
type FieldValueList = string[]

type StreamMessage = [MessageId, FieldValueList]
type StreamMessages = StreamMessage[]

type RedisStreamEntry = [StreamName, StreamMessages]

export type RedisXReadGroupResponse = RedisStreamEntry[]

const redis = new Redis(process.env.REDIS_URL)
const clickhouse = createClient({ url: process.env.CLICKHOUSE_URL })
const CONSUMER_NAME = process.env.WORKER_NAME || os.hostname()

async function processEvents() {
  while (true) {
    try {
      const result = await redis.xreadgroup(
        "GROUP",
        "worker_group",
        CONSUMER_NAME,
        "BLOCK",
        0,
        "STREAMS",
        "events_stream",
        ">"
      )

      if (!result) continue

      for (const [, messages] of result as RedisXReadGroupResponse) {
        for (const [id, fields] of messages) {
          const event = JSON.parse(fields[1])

          await clickhouse.insert({
            table: "events",
            values: [
              {
                event_type: event.type,
                user_id: event.userId,
                timestamp: event.timestamp,
                metadata: JSON.stringify(event.metadata || {})
              }
            ]
          })
        }
      }
    } catch (error) {
      console.error("ERROR PROCESSING EVENTS:", error)
      await new Promise((resolve) => setTimeout(resolve, 1000))
    }
  }
}

processEvents().catch((error) => {
  console.error("FATAL ERROR IN WORKER:", error)
  process.exit(1)
})
