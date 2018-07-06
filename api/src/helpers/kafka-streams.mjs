import KafkaStreams from 'kafka-streams';

export default new KafkaStreams.KafkaStreams({
  //zkConStr: "localhost:2181/",
  kafkaHost: "localhost:29092", //either kafkaHost or zkConStr
  logger: {
    debug: msg => console.log(msg),
    info: msg => console.log(msg),
    warn: msg => console.log(msg),
    error: msg => console.error(msg)
  },
  groupId: "kafka-streams-test",
  clientName: "kafka-streams-test-name",
  workerPerPartition: 1,
  options: {
    sessionTimeout: 8000,
    protocol: ["roundrobin"],
    fromOffset: "earliest", //latest
    fetchMaxBytes: 1024 * 100,
    fetchMinBytes: 1,
    fetchMaxWaitMs: 10,
    heartbeatInterval: 250,
    retryMinTimeout: 250,
    autoCommit: true,
    autoCommitIntervalMs: 1000,
    requireAcks: 1,
    ackTimeoutMs: 100,
    partitionerType: 3
  }
});
