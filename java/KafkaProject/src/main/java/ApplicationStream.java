import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.common.serialization.Serdes;



import java.io.IOException;

public class ApplicationStream extends StreamingConfig{

    public static void main(String[] args){

        ApplicationStream main = new ApplicationStream();
        main.run();
    }

    public JsonNode parse(String stringToParse){
        ObjectMapper mapper = new ObjectMapper();

        try {
            JsonNode tree = mapper.readTree(stringToParse);
            return tree;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;

    }

    public JsonNode print(JsonNode stringToParse){
        System.out.println(stringToParse.asText());
        return stringToParse;
    }


    private void run() {

        Serde<String> stringSerde = Serdes.String();
        Serde<byte[]> byteArraySerde = Serdes.ByteArray();

        ObjectMapper mapper = new ObjectMapper();

        StreamsBuilder builder = new StreamsBuilder();
        KStream<byte[], String> message = builder.stream("twitter", Consumed.with(byteArraySerde, stringSerde));
        KStream<byte[], JsonNode> json_message = message.map((key, value) -> new KeyValue<>(key, parse(value)));
        KStream<byte[], JsonNode> test = json_message.mapValues((value)-> YOUR_FUCKING_FUNCTION(value.get("message").asText()));



        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();


    }

}
