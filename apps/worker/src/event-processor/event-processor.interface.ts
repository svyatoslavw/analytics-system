import { RedisXReadGroupResponse } from "@/types"

export interface IEventProcessor {
  initialize(): Promise<void>
  processEvents(events: RedisXReadGroupResponse): Promise<void>
}
