import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.common.serialization.Serdes;
//import sun.plugin2.message.Serializer;


import javax.json.Json;
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

        Deserializer<JsonNode> jsonNodeDeserializer = new JsonDeserializer();
        Serializer<JsonNode> jsonSerializer= new JsonSerializer();

        Serde<String> stringSerde = Serdes.String();
        Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonNodeDeserializer);


        ObjectMapper mapper = new ObjectMapper();

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> message = builder.stream("twitter", Consumed.with(stringSerde, stringSerde));
        KStream<String, JsonNode> json_message = message.mapValues((value) -> {
             JsonNode node = parse(value);
             int sentiment = findSentiment(node.get("message").asText());
            ObjectNode test_copy = node.deepCopy();
            test_copy.put("sentiment",sentiment);
            JsonNode casted_json_message = ((JsonNode) test_copy);

            return casted_json_message;

        });

        KGroupedStream<String, JsonNode> twitter_group_by_user = json_message.groupBy((key, value) -> value.get("nickname").asText());

        KTable<String, JsonNode> group_user_twitter = twitter_group_by_user.aggregate(
                () -> {
                    ObjectNode node = JsonNodeFactory.instance.objectNode();
                    node.put("negative", 0);
                    node.put("neutral", 0);
                    node.put("positive",0);

                    return node;

                },
                (key, value, aggregate) -> {
                    ObjectNode ag = aggregate.deepCopy();

                    if (value.get("sentiment").asInt()==2){
                        ag.put("neutral", ag.get("neutral").asLong()+1);
                    }

                    else if (value.get("sentiment").asInt()<2){
                        ag.put("negative", ag.get("negative").asLong()+1);
                    }
                    else {
                        ag.put("positive", ag.get("positive").asLong()+1);
                    }
                    //JsonNode ag_j = ((JsonNode) ag);
                    return ag;
                }, Materialized.with(stringSerde, jsonSerde));
        //group_user_twitter.to(Serdes.String(), jsonSerde, "twitter_group_by_user");






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
