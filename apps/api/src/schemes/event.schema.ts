import { z } from "zod"

export const EventSchema = z.object({
  eventId: z.uuid(),
  eventName: z.string(),
  userId: z.string(),
  timestamp: z.string().refine((date) => !isNaN(Date.parse(date)), {
    message: "Invalid date format"
  }),
  metadata: z.record(z.any(), z.any()).optional()
})

export const createEventSchema = EventSchema.omit({ eventId: true })

export type EventType = z.infer<typeof EventSchema>
export type CreateEventType = z.infer<typeof createEventSchema>
