import Kafka from 'kafka-node';

const client = new Kafka.KafkaClient({kafkaHost:'localhost:29092'});

export default client;