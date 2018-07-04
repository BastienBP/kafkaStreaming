import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.common.serialization.Serdes;



import java.io.IOException;
import java.util.Properties;

public class ApplicationStream extends StreamingConfig {

    private StanfordCoreNLP pipeline;

    public static void main(String[] args) {

        ApplicationStream main = new ApplicationStream();
        main.run();
    }

    public ApplicationStream() {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize,ssplit,pos,parse,sentiment");
        this.pipeline = new StanfordCoreNLP(props);
    }

    public JsonNode parse(String stringToParse) {
        ObjectMapper mapper = new ObjectMapper();

        try {
            JsonNode tree = mapper.readTree(stringToParse);
            return tree;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;

    }

    private void run() {

        Serde<String> stringSerde = Serdes.String();
        Serde<byte[]> byteArraySerde = Serdes.ByteArray();

        ObjectMapper mapper = new ObjectMapper();

        StreamsBuilder builder = new StreamsBuilder();
        KStream<byte[], String> message = builder.stream("twitter", Consumed.with(byteArraySerde, stringSerde));
        KStream<byte[], JsonNode> json_message = message.map((key, value) -> new KeyValue<>(key, parse(value)));
        KStream<byte[], Integer> test = json_message.mapValues((value)-> findSentiment(value.get("message").asText()));
        test.print();

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
    }


    public int findSentiment(String tweet) {
        int mainSentiment = 0;
        if (tweet != null && tweet.length() > 0) {
            int longest = 0;
            Annotation annotation = this.pipeline.process(tweet);
            for (CoreMap sentence : annotation
                    .get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence
                        .get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = sentence.toString();
                if (partText.length() > longest) {
                    mainSentiment = sentiment;
                    longest = partText.length();
                }

            }
        }
        return mainSentiment;
    }
}
