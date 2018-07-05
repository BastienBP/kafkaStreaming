
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.common.metrics.Sensor;

public abstract class StreamingConfig{

    protected StreamsConfig config;

    protected StreamingConfig(){

        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka.streaming");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:32181");
        settings.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        //settings.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        settings.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        //settings.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "latest");
        settings.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

        config = new StreamsConfig(settings);

    }


}