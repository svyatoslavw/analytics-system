import os from "os"

interface AppConfig {
  redisUrl: string
  clickhouseUrl: string
  streamName: string
  consumerGroup: string
  consumerName: string
  batchSize: number
  pollInterval: number
  maxRetries: number
  healthCheckPort?: number
}

export const config: AppConfig = {
  redisUrl: process.env.REDIS_URL || "redis://localhost:6379",
  clickhouseUrl: process.env.CLICKHOUSE_URL || "http://localhost:8123",
  streamName: process.env.STREAM_NAME || "events_stream",
  consumerGroup: process.env.CONSUMER_GROUP || "worker_group",
  consumerName: process.env.WORKER_NAME || os.hostname(),
  batchSize: parseInt(process.env.BATCH_SIZE || "100"),
  pollInterval: parseInt(process.env.POLL_INTERVAL || "1000"),
  maxRetries: parseInt(process.env.MAX_RETRIES || "3"),
  healthCheckPort: process.env.HEALTH_CHECK_PORT
    ? parseInt(process.env.HEALTH_CHECK_PORT)
    : undefined
}
