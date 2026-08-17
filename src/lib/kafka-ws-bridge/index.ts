/**
 * Kafka ↔ WebSocket bridge public API.
 */

export { startKafkaWsBridge } from "./start-bridge";
export {
  isAckRequiredTopic,
  isConsumerExcludedTopic,
  getConsumerTopicPattern,
  getConsumerExcludedTopics,
  getAckRequiredTopics,
  getSubscriberPublishAllowlist,
} from "./record-log-topics";
export {
  registerPendingAck,
  markPendingDelivered,
  commitByCorrelationId,
  countPendingForSubscriber,
  _testGetPendingAck,
  _testHasPendingAck,
  _testPendingCount,
} from "./pending-acks";
export { registerWs, unregisterWs } from "./ws-registry";
export { wsClients, pendingAcks } from "./state";
export {
  pickResponsibleSubscriber,
  waitForResponsibleSubscriber,
} from "./subscriber-selection";
export { shouldCommitAfterAck, waitForAckWithHeartbeat } from "./ack-wait";
export { decodeKafkaHeaders } from "./headers";
