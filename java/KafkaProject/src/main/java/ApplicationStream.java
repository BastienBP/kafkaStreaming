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
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;


import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

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
        Serializer<JsonNode> jsonSerializer = new JsonSerializer();

        Serde<Long> longSerde = Serdes.Long();
        Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonNodeDeserializer);

        StringSerializer stringSerializer = new StringSerializer();
        StringDeserializer stringDeserializer = new StringDeserializer();
        Serde<String> stringSerde = Serdes.serdeFrom(stringSerializer, stringDeserializer);
        WindowedSerializer<String> windowedSerializer = new WindowedSerializer<>(stringSerializer);
        WindowedDeserializer<String> windowedDeserializer = new WindowedDeserializer<>(stringDeserializer);
        Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> rawTweets = builder.stream("raw_tweets", Consumed.with(stringSerde, stringSerde));

        KStream<String, JsonNode> tweetsWithRawSentiments = rawTweets.mapValues((value) -> {
            JsonNode node = parse(value);
            int sentiment = findSentiment(node.get("message").asText());
            ObjectNode objectNode = node.deepCopy();
            objectNode.put("sentiment", sentiment);
            return objectNode;
        });

        KStream<String, Long> hashtagCount = rawTweets
                .flatMap((key, value) -> {
                    JsonNode node = parse(value);
                    List<String> hashtags = extractHashtags(node.get("message").asText());
                    List<KeyValue<String, Long>> hashtagsKVs = hashtags.stream().map(hashtag -> (new KeyValue<>(hashtag, 1L))).collect(Collectors.toList());
                    return hashtagsKVs;
                })
                .groupByKey(Serialized.with(stringSerde, longSerde))
                .count()
                .toStream();

        hashtagCount.to("hashtag_count", Produced.with(stringSerde, longSerde));

        tweetsWithRawSentiments
                .<JsonNode>mapValues(value -> {
                    ObjectNode node = value.deepCopy();
                    node.put("sentiment", mapSentiment(node.get("sentiment").asInt()));
                    return node;
                })
                .to("analyzed_tweets", Produced.with(stringSerde, jsonSerde));

        KGroupedStream<String, JsonNode> tweetsGroupedByUser = tweetsWithRawSentiments
                .groupBy((key, value) -> value.get("nickname").asText(), Serialized.with(stringSerde, jsonSerde));


        TimeWindowedKStream<String, JsonNode> tweetsGroupedByHour = tweetsWithRawSentiments
                .groupBy((key, value) -> value.get("sentiment").asText(), Serialized.with(stringSerde, jsonSerde))
                .windowedBy(TimeWindows.of(1 * 3600 * 1000));

        KTable<Windowed<String>, JsonNode> group_hour_twitter = tweetsGroupedByHour.aggregate(
                () -> {
                    ObjectNode node = JsonNodeFactory.instance.objectNode();
                    node.put("negative", 0);
                    node.put("neutral", 0);
                    node.put("positive", 0);

                    return node;
                },
                (key, value, aggregate) -> {
                    ObjectNode ag = aggregate.deepCopy();

                    if (value.get("sentiment").asInt() == 2) {
                        ag.put("neutral", ag.get("neutral").asLong() + 1);
                    } else if (value.get("sentiment").asInt() < 2) {
                        ag.put("negative", ag.get("negative").asLong() + 1);
                    } else {
                        ag.put("positive", ag.get("positive").asLong() + 1);
                    }
                    ag.put("date", value.get("timestamp"));

                    return ag;
                },
                Materialized.with(stringSerde, jsonSerde));

        group_hour_twitter.to(windowedSerde, jsonSerde, "sentiments_hour");


        // Time windows by year

        TimeWindowedKStream<String, JsonNode> tweetsGroupedByYear = tweetsWithRawSentiments
                .groupBy((key, value) -> value.get("sentiment").asText(), Serialized.with(stringSerde, jsonSerde))
                .windowedBy(TimeWindows.of(31104000000L));

        KTable<Windowed<String>, JsonNode> group_year_twitter = tweetsGroupedByYear.aggregate(
                () -> {
                    ObjectNode node = JsonNodeFactory.instance.objectNode();
                    node.put("negative", 0);
                    node.put("neutral", 0);
                    node.put("positive", 0);

                    return node;
                },
                (key, value, aggregate) -> {
                    ObjectNode ag = aggregate.deepCopy();

                    if (value.get("sentiment").asInt() == 2) {
                        ag.put("neutral", ag.get("neutral").asLong() + 1);
                    } else if (value.get("sentiment").asInt() < 2) {
                        ag.put("negative", ag.get("negative").asLong() + 1);
                    } else {
                        ag.put("positive", ag.get("positive").asLong() + 1);
                    }
                    ag.put("date", value.get("timestamp"));
                    return ag;
                },
                Materialized.with(stringSerde, jsonSerde));

        group_hour_twitter.to(windowedSerde, jsonSerde, "sentiments_year");

        // Time windows by month

        TimeWindowedKStream<String, JsonNode> tweetsGroupedByMonth = tweetsWithRawSentiments
                .groupBy((key, value) -> value.get("sentiment").asText(), Serialized.with(stringSerde, jsonSerde))
                .windowedBy(TimeWindows.of(2592000000L));

        KTable<Windowed<String>, JsonNode> group_month_twitter = tweetsGroupedByMonth.aggregate(
                () -> {
                    ObjectNode node = JsonNodeFactory.instance.objectNode();
                    node.put("negative", 0);
                    node.put("neutral", 0);
                    node.put("positive", 0);

                    return node;
                },
                (key, value, aggregate) -> {
                    ObjectNode ag = aggregate.deepCopy();

                    if (value.get("sentiment").asInt() == 2) {
                        ag.put("neutral", ag.get("neutral").asLong() + 1);
                    } else if (value.get("sentiment").asInt() < 2) {
                        ag.put("negative", ag.get("negative").asLong() + 1);
                    } else {
                        ag.put("positive", ag.get("positive").asLong() + 1);
                    }
                    ag.put("date", value.get("timestamp"));
                    return ag;
                },
                Materialized.as("sentiments_month").with(stringSerde, jsonSerde));

        group_month_twitter.to(windowedSerde, jsonSerde, "sentiments_month");


        // Time windows by month

        TimeWindowedKStream<String, JsonNode> tweetsGroupedByDay = tweetsWithRawSentiments
                .groupBy((key, value) -> value.get("sentiment").asText(), Serialized.with(stringSerde, jsonSerde))
                .windowedBy(TimeWindows.of(1 * 3600 * 24 * 1000));

        KTable<Windowed<String>, JsonNode> group_day_twitter = tweetsGroupedByDay.aggregate(
                () -> {
                    ObjectNode node = JsonNodeFactory.instance.objectNode();
                    node.put("negative", 0);
                    node.put("neutral", 0);
                    node.put("positive", 0);

                    return node;
                },
                (key, value, aggregate) -> {
                    ObjectNode ag = aggregate.deepCopy();

                    if (value.get("sentiment").asInt() == 2) {
                        ag.put("neutral", ag.get("neutral").asLong() + 1);
                    } else if (value.get("sentiment").asInt() < 2) {
                        ag.put("negative", ag.get("negative").asLong() + 1);
                    } else {
                        ag.put("positive", ag.get("positive").asLong() + 1);
                    }
                    ag.put("date", value.get("timestamp"));
                    return ag;
                },
                Materialized.with(stringSerde, jsonSerde));

        group_day_twitter.to(windowedSerde, jsonSerde, "sentiments_day");


        KTable<String, JsonNode> group_user_twitter = tweetsGroupedByUser.aggregate(
                () -> {
                    ObjectNode node = JsonNodeFactory.instance.objectNode();
                    node.put("negative", 0);
                    node.put("neutral", 0);
                    node.put("positive", 0);
                    return node;
                },
                (key, value, aggregate) -> {
                    ObjectNode ag = aggregate.deepCopy();

                    if (value.get("sentiment").asInt() == 2) {
                        ag.put("neutral", ag.get("neutral").asLong() + 1);
                    } else if (value.get("sentiment").asInt() < 2) {
                        ag.put("negative", ag.get("negative").asLong() + 1);
                    } else {
                        ag.put("positive", ag.get("positive").asLong() + 1);
                    }

                    return ag;
                }, Materialized.with(stringSerde, jsonSerde));

        group_user_twitter.toStream().to("sentiments_all_user", Produced.with(stringSerde, jsonSerde));


        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
    }

    private String mapSentiment(int sentiment) {
        // "Very negative" = 0 "Negative" = 1 "Neutral" = 2 "Positive" = 3 "Very positive" = 4
        switch (sentiment) {
            case 0:
                return "Very negative";
            case 1:
                return "Negative";
            case 2:
                return "Neutral";
            case 3:
                return "Positive";
            case 4:
                return "Very positive";
            default:
                throw new InvalidParameterException("Invalid sentiment");
        }
    }

    private List<String> extractHashtags(String message) {
        List<String> words = Arrays.asList(message.split("\\s+"));
        List<String> hastags = words.stream().filter(word -> word.startsWith("#")).collect(Collectors.toList());
        return hastags;
    }

    private int findSentiment(String tweet) {
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
