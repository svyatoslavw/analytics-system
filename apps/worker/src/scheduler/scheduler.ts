import { logger } from "@/common/logger"
import { schedule, type ScheduledTask } from "node-cron"
import { IScheduler } from "./scheduler.interface"

export class Scheduler implements IScheduler {
  private tasks: Map<string, NodeJS.Timeout> = new Map()
  private cronJobs: Map<string, ScheduledTask> = new Map()

  scheduleInterval(name: string, intervalMs: number, task: () => Promise<void>) {
    const intervalId = setInterval(async () => {
      try {
        await task()
      } catch (error) {
        logger.error(`Interval task "${name}" failed: ${error}`)
      }
    }, intervalMs)
    this.tasks.set(name, intervalId)
    logger.info(`Scheduled interval task "${name}" to run every ${intervalMs} ms`)
  }

  scheduleCron(name: string, expression: string, task: () => Promise<void>) {
    const job = schedule(expression, async () => {
      try {
        await task()
      } catch (error) {
        logger.error(`Cron job "${name}" failed: ${error}`)
      }
    })
    this.cronJobs.set(name, job)
    job.start()
    logger.info(`Scheduled cron task "${name}" with expression "${expression}"`)
  }

  stopAll() {
    this.tasks.forEach((intervalId, name) => {
      clearInterval(intervalId)
      logger.info(`Stopped interval task "${name}"`)
    })
    this.cronJobs.forEach((job, name) => {
      job.stop()
      logger.info(`Stopped cron task "${name}"`)
    })
    this.tasks.clear()
    this.cronJobs.clear()
  }
}
