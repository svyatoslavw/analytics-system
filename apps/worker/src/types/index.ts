type StreamName = string
type MessageId = string
type FieldValueList = string[]

type StreamMessage = [MessageId, FieldValueList]
type StreamMessages = StreamMessage[]

type RedisStreamEntry = [StreamName, StreamMessages]

export type RedisXReadGroupResponse = RedisStreamEntry[]
