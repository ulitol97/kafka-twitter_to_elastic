package tutorial;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    private static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
    private final String CONFIG_FILENAME = getClass().getResource("/config/auth_data.txt").getPath();
    private RestHighLevelClient client;

    public static void main(String[] args) {
        new ElasticSearchConsumer().run();
    }

    private void run () {
        // Create client
        client = createClient();

        // Create kafka consumer
        KafkaConsumer<String, String> consumer = createConsumer(new String[]{"twitter-tweets"});

        // Consumer poll for data
        ConsumerRecords<String, String> records;
        while (true){
            records = consumer.poll(Duration.ofMillis(100)); // Timeout length
            logger.info("Received " + records.count() + " records to be processed.");

            // Bulk requests process a number if index requests altogether to earn time
            BulkRequest bulkRequest = new BulkRequest();
            // For each record, insert its data into elastic
            for (ConsumerRecord<String, String> record : records){
                    try {
                        bulkRequest.add(CreateInsertRequest(record.value()));
                    }
                    catch (NullPointerException e){
                        logger.warn("Skipping bad data (no tweet id found):\n" + record.value());
                }
            }

            // Execute the bulk request if there's data to send
            if (bulkRequest.requests().size() > 0){
                logger.info("Running block of requests (" + bulkRequest.requests().size() + " requests)");
                try {
                    client.bulk(bulkRequest, RequestOptions.DEFAULT);
                } catch (IOException e) {
                    e.printStackTrace();
                    logger.error("Error in request");
                }
            }

            // The records consumed have been processed (inserted into elastic), we should commit consumer offsets
            consumer.commitSync();
            logger.info("Consumer offsets committed");
        }

//        client.close();
    }

    // Send data to elastic
    private IndexRequest CreateInsertRequest(String jsonValue) {

        // Type is "_doc".
        IndexRequest indexRequest = new IndexRequest("twitter");
        indexRequest.source(jsonValue, XContentType.JSON);

        // We tell elastic which id to use to avoid duplicates. We'll use the tweet IDs as Elastic IDs
        // It is common to use Topic_Partition_Offset as a unique ID too.
        indexRequest.id(ExtractIdFromTweet(jsonValue));

        // Return the request ready to be run
        return indexRequest;
    }

    private String ExtractIdFromTweet(String tweetJson) {
        // Use Gson library to get the tweet ID from the Json object with the whole tweet data
        return JsonParser.parseString(tweetJson).getAsJsonObject().get("id_str").getAsString();
    }

    // Create Kafka Consumer
    private KafkaConsumer<String, String> createConsumer (String[] topics) {
        // Consumer properties
        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Consume from the beginning of topic
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // We'll commit read offsets manually
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); // Get a maximum of N records per poll

        // Config idempotence


        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topics));
        return consumer;
    }


    // Create ElasticSearch client
    private RestHighLevelClient createClient() {

        // Read credentials from file
        String [] authData = ClientConfiguration.loadAuthData(CONFIG_FILENAME);
        String hostname = authData[0];
        String username = authData[1];
        String password = authData[2];

        // Needed this to auth in cloud elastic cluster
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        // Elastic client builder
        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(
                        httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        return new RestHighLevelClient(builder);
    }
}

class ClientConfiguration {

    private static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    // Load 3 pieces of data needed to authenticate the client in the cloud
    static String[] loadAuthData(String filename) {
        String[] authData = new String[3];
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            for (int i = 0; i < authData.length; i++){
                line = br.readLine();
                if (line != null)
                    authData[i] = line;
                else // No info to read
                    authData[i] = "";
            }
        }
        catch (IOException e){
            e.printStackTrace();
        }
        logger.info("Loaded auth configuration from file " + filename);
        return authData;
    }
}
