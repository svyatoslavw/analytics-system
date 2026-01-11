export interface IScheduler {
  scheduleInterval(name: string, intervalMs: number, task: () => Promise<void>): void
  scheduleCron(name: string, expression: string, task: () => Promise<void>): void
  stopAll(): void
}
