import Fastify, { FastifyRequest } from "fastify"
import Redis from "ioredis"

import * as dotenv from "dotenv"
import { createEventSchema, CreateEventType } from "./schemes"
dotenv.config()

const client = new Redis(process.env.REDIS_URL)

const server = Fastify({
  logger: true
})

server.get("/", (request, reply) => {
  console.log(process.env.REDIS_URL)
  reply.send({ hello: "world" })
})

server.post("/events", async (request: FastifyRequest<{ Body: CreateEventType }>, reply) => {
  const { success, error } = createEventSchema.safeParse(request.body)

  if (!success) {
    return reply.status(400).send({ error: "Invalid event data", cause: error })
  }

  const event = request.body

  await client.xadd("events_stream", "*", "data", JSON.stringify(event))

  console.log("Received event:", event)

  reply.send({ status: "ok" })
})

server.listen({ port: 4000, host: "0.0.0.0" }, (err, address) => {
  if (err) {
    server.log.error(err)
    process.exit(1)
  }
})
