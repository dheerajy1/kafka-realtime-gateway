# WebSocket protocol

The gateway has two WebSocket endpoints. OpenAPI cannot describe WebSocket messages, so this file is the protocol reference. The HTTP endpoints are in the OpenAPI reference at `/openapi`.

| Endpoint | Purpose |
| --- | --- |
| `GET /api/v1/ws-subscribe` | Receive Kafka messages, acknowledge them, and publish. |
| `GET /api/v1/ws-publish` | Publish jobs to Kafka (job style). |

## Connecting

Both endpoints upgrade with a normal HTTP `GET` that carries these headers:

| Header | Required | Meaning |
| --- | --- | --- |
| `x-client-id` | both | Client id (`CLIENT_ID`). |
| `x-client-secret` | both | Client secret (`CLIENT_SECRET`). |
| `x-api-key` | both | API key id. |
| `x-api-secret` | both | API key secret. |
| `x-subscriber-id` | `ws-subscribe` only | Name of the subscriber, 1 to 44 characters, used in logs. |

A missing, empty or too long `x-subscriber-id` is answered with HTTP 400 before the upgrade.

The credentials are checked after the upgrade. If they fail, the server sends one error frame and closes with code 4001:

```json
{ "type": "error", "success": false, "statusCode": 401, "code": "...", "message": "...", "error": "..." }
```

If they pass, the server sends a `ready` frame. `ws-subscribe` sends `{"type":"ready"}`. `ws-publish` sends `{"type":"ready","message":"WebSocket publish connection established"}`.

All frames are JSON text. A binary frame is answered with `{"type":"error","message":"Malformed JSON"}`.

## ws-subscribe

### Client to server

| Message | Meaning |
| --- | --- |
| `{"type":"subscribe","topic":"t"}` | Start receiving topic `t`. The topic is not checked against the registry. A topic nobody consumes never delivers. |
| `{"type":"unsubscribe","topic":"t"}` | Stop receiving topic `t`. |
| `{"type":"processed","correlationId":"<uuid v7>"}` | Acknowledge a message you finished (see ACK topics). |
| `{"type":"publish","requestId":"<uuid>","topic":"t","key":"k","value":{...},"headers":{"a":"b"}}` | Publish to Kafka. `key` and `headers` are optional, `key` must not be empty, `value` must be an object. The topic must have `subscriber_publish_allowed`. |

### Server to client

| Frame | Meaning |
| --- | --- |
| `{"type":"subscribed","topic":"t"}` | Subscribe accepted. |
| `{"type":"unsubscribed","topic":"t"}` | Unsubscribe accepted. |
| `{"type":"event","topic":"t","partition":0,"offset":"42","headers":{...},...payload}` | A Kafka message. The offset is a string. Every field of the Kafka message value is merged into the frame, and the value fields win over the fixed ones. |
| `{"type":"ack-received","correlationId":"..."}` | Your `processed` was accepted. |
| `{"type":"published","requestId":"...","topic":"t","partition":0,"offset":"42"}` | Your `publish` reached Kafka. |
| `{"type":"error","message":"Malformed JSON"}` | The text is not JSON. |
| `{"type":"error","message":"Invalid message format","issues":[{"message":"..."}]}` | The JSON has the wrong shape. |
| `{"type":"error","message":"ACK rejected","reason":"...","correlationId":"..."}` | Reasons: `offset not found or already committed`, `ACK from non-owning connection`. |
| `{"type":"error","message":"Unauthorized topic","reason":"topic-not-allowed","requestId":"..."}` | The topic does not allow subscriber publishing. |
| `{"type":"error","message":"Kafka publish failed","reason":"...","requestId":"..."}` | Kafka refused or failed the publish. |

### What the gateway delivers

The gateway reads the topics that are enabled and marked `gateway_consume` in `kafka_topic`. Every Kafka message value must be a JSON object with a `correlationId` that is a UUID v7. Empty, malformed and invalid messages are skipped and logged.

Topics without `ack_required`: the message goes to every subscriber of the topic. With no subscriber, it is dropped.

### ACK topics

On a topic with `ack_required`, the gateway sends the message to one subscriber, the owner (the connected subscriber with the lowest id). The gateway then waits up to 5 minutes for `{"type":"processed","correlationId":"..."}` from that same connection. Only then is the Kafka offset committed.

- No subscriber connected: the gateway waits for one, and the message stays in Kafka.
- The owner disconnects, or the 5 minutes pass without an ACK: the message is read again and delivered again.
- An ACK from a connection that does not own the message is rejected.

So a subscriber on an ACK topic must tolerate the same message arriving more than once.

## ws-publish

Client to server: `{"topic":"t","jobName":"name"}`. Both fields are required and must not be empty. Other fields are ignored.

The topic must be enabled in the topic registry. The gateway publishes this value, with the API key id as the Kafka key:

```json
{ "correlationId": "<uuid v7>", "apiUserId": "...", "jobName": "name", "timestamp": "2026-11-03T05:12:40.569Z" }
```

| Server frame | Meaning |
| --- | --- |
| `{"type":"ack","topic":"t","correlationId":"..."}` | Published to Kafka. It does not mean processed. |
| `{"type":"error","message":"Malformed JSON"}` | The text is not JSON. |
| `{"type":"error","message":"Invalid publish message"}` | A field is missing or empty. |
| `{"type":"error","message":"Unauthorized topic","reason":"topic-not-allowed"}` | The topic is not enabled. |
| `{"type":"error","message":"Kafka publish failed","reason":"..."}` | Kafka refused or failed the publish. |

## Limits and keepalive

- Every message and frame is limited to 10 MB. A larger message ends the connection.
- The gateway sends a WebSocket ping every 30 seconds. Any frame from the client counts as an answer. A client that stays silent for one more period is dropped without a close frame.
- Each subscriber has a queue of 1000 frames. A subscriber that falls behind is closed with 1013 and may reconnect.

## Close codes

| Code | Meaning |
| --- | --- |
| 1001 | The gateway is shutting down. Reconnect later. |
| 1013 | The subscriber was too slow. It may reconnect. |
| 4001 | Authentication failed. |

## Trying it

`cargo test-ws` runs a scenario that covers all of this against a running gateway. See [dev-setup.md](dev-setup.md).