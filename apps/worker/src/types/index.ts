type StreamName = string
type MessageId = string
type FieldValueList = string[]

type StreamMessage = [MessageId, FieldValueList]
type StreamMessages = StreamMessage[]

export type RedisStreamEntry = [StreamName, StreamMessages]

export type RedisXReadGroupResponse = RedisStreamEntry[]
