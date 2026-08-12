/**
 * Pipeline State Consumer — WebSocket subscriber worker.
 * Does NOT own a Kafka consumer. Does NOT commit Kafka offsets.
 */

import { isoNowIST } from "@/lib/isoNowIST";
import { env } from "@/lib/env";
import { createPostgresDb } from "@/pipeline-state-consumer/lib/postgres";
import { processStatusMessage } from "@/pipeline-state-consumer/lib/processor";
import WebSocket from "ws";

const GATEWAY_WS_URL = `${env.PIPELINE_STATE_CONSUMER_WS_URL.replace(/^https/, "ws")}/api/v1/ws-subscribe`;
const TOPIC = env.PIPELINE_STATE_CONSUMER_TOPIC;
const CLIENT_ID = env.PIPELINE_STATE_CONSUMER_CLIENT_ID;
const API_KEY = env.PIPELINE_STATE_CONSUMER_API_KEY;
const API_KEY_SECRET = env.PIPELINE_STATE_CONSUMER_API_SECRET;
const RECONNECT_BASE_MS = 1_000;
const RECONNECT_MAX_MS = 30_000;

let shuttingDown = false;
let reconnectAttempt = 0;
let ws: WebSocket | null = null;
const db = createPostgresDb();

function scheduleReconnect() {
  if (shuttingDown) return;
  const delay = Math.min(
    RECONNECT_BASE_MS * 2 ** reconnectAttempt,
    RECONNECT_MAX_MS,
  );
  reconnectAttempt += 1;
  console.log(
    `${isoNowIST()}\t[pipeline-state]\t reconnect in ${delay}ms (attempt ${reconnectAttempt})`,
  );
  setTimeout(() => {
    if (!shuttingDown) connect();
  }, delay);
}

function connect() {
  if (shuttingDown) return;

  console.log(
    `${isoNowIST()}\t[pipeline-state]\t connecting WS ${GATEWAY_WS_URL} as ${CLIENT_ID}`,
  );

  const socket = new WebSocket(GATEWAY_WS_URL, {
    headers: {
      "x-client-id": env.CLIENT_ID,
      "x-client-secret": env.CLIENT_SECRET,
      "x-api-key": API_KEY,
      "x-api-secret": API_KEY_SECRET,
    },
  });

  ws = socket;

  socket.on("open", () => {
    reconnectAttempt = 0;
    console.log(`${isoNowIST()}\t[pipeline-state]\t WS connected`);
  });

  socket.on("message", async (data) => {
    let msg: {
      type?: string;
      topic?: string;
      correlationId?: string;
      [k: string]: unknown;
    };
    try {
      msg = JSON.parse(data.toString()) as typeof msg;
    } catch {
      console.error(`${isoNowIST()}\t[pipeline-state]\t malformed JSON`);
      return;
    }

    if (msg.type === "ready") {
      socket.send(JSON.stringify({ type: "subscribe", topic: TOPIC }));
      console.log(
        `${isoNowIST()}\t[pipeline-state]\t subscribed topic=${TOPIC}`,
      );
      return;
    }

    if (msg.type === "subscribed") {
      console.log(
        `${isoNowIST()}\t[pipeline-state]\t confirmed subscribed topic=${msg.topic}`,
      );
      return;
    }

    if (msg.type === "event") {
      const correlationId = msg.correlationId ?? "?";
      console.log(
        `${isoNowIST()}\t[pipeline-state]\t EVENT corr=${correlationId} topic=${msg.topic}`,
      );

      const { type: _t, topic: _topic, ...payload } = msg;
      const raw = JSON.stringify(payload);
      const result = await processStatusMessage(raw, { db });

      if (!result.ok) {
        console.error(
          `${isoNowIST()}\t[pipeline-state]\t FAIL corr=${result.correlationId ?? correlationId} retryable=${result.retryable}: ${result.error}`,
        );
        return;
      }

      socket.send(
        JSON.stringify({
          type: "processed",
          correlationId: result.correlationId,
        }),
      );

      console.log(
        `${isoNowIST()}\t[pipeline-state]\t OK corr=${result.correlationId} stage=${result.stage} seq=${result.sequence} replayed=${result.replayed}`,
      );
      return;
    }

    if (msg.type === "error") {
      console.error(
        `${isoNowIST()}\t[pipeline-state]\t CONTROL error`,
        data.toString(),
      );
      return;
    }

    console.log(`${isoNowIST()}\t[pipeline-state]\t CONTROL`, data.toString());
  });

  socket.on("close", () => {
    console.log(`${isoNowIST()}\t[pipeline-state]\t WS closed`);
    ws = null;
    scheduleReconnect();
  });

  socket.on("error", (err) => {
    console.error(`${isoNowIST()}\t[pipeline-state]\t WS error`, err);
  });
}

async function shutdown(signal: string) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`${isoNowIST()}\t[pipeline-state]\t shutting down (${signal})`);
  try {
    if (ws && ws.readyState === WebSocket.OPEN) ws.close();
    await db.close();
  } finally {
    process.exit(0);
  }
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));

connect();
