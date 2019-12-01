package tutorial;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {
    public static void main(String[] args) {
        // create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); // local
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");

            // Strings as keys and values by default
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // input topic to be read
        KStream<String, String> inputTopicStreamBuilder = streamsBuilder.stream("twitter-tweets");
        KStream<String, String> filteredStream = inputTopicStreamBuilder.filter(
                (key, value) -> {
                    // Filter popular tweets (10k+ followers)
                    return ExtractFollowersFromTweet(value) >= 10000;
                }
        );

        // Send the filtered data to another topic
        filteredStream.to("important-tweets");

        // build the topology
        KafkaStreams kafkaStreams = new KafkaStreams( streamsBuilder.build(), properties);

        // start streams app
        kafkaStreams.start();
    }

    private static int ExtractFollowersFromTweet(String tweetJson) {
        // Use Gson library to get the tweet followers from the Json object with the whole tweet data
        try {
            return JsonParser.parseString(tweetJson).getAsJsonObject().get("user").getAsJsonObject()
                    .get("followers_count").getAsInt();
        }
        catch (NullPointerException e){
            return 0;
        }
    }
}
