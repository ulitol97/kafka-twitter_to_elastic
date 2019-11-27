package com.ragna.kafka.tutorial;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    private final String CONFIG_FILENAME = getClass().getResource("/config/auth_data.txt").getPath();
    private final String FOLLOW_FILENAME = getClass().getResource("/config/follow_people.txt").getPath();
    private final String TERMS_FILENAME = getClass().getResource("/config/follow_topic.txt").getPath();

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    private TwitterProducer() {

    }

    private void run() {
        // Create Twitter Client

        /* Blocking queues: where the data fetched will be allocated */
        logger.info("Setting up Twitter client");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
        Client client = createTwitterClients(msgQueue);
        logger.info("Twitter client ready");
        client.connect();
        logger.info("Twitter client connected!");

        // Create Kafka producer with tweet data

        // loop to poll for new tweets to send to kafka

        // test: send tweets to console kafka consumer
        while (!client.isDone()){
            try {
                String msg = msgQueue.poll(5, TimeUnit.SECONDS);
                logger.info(msg);
            } catch (InterruptedException e) {
                logger.error("Error retrieving data");
                e.printStackTrace();
                client.stop();
            }
        }
        logger.info("End of application.");
    }

    private Client createTwitterClients(BlockingQueue<String> msgQueue) {

        // Client setup

        /* Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Set up some followings of people or terms
        List<Long> followings = ClientConfiguration.loadFollowPeopleData(FOLLOW_FILENAME);
        List<String> terms = ClientConfiguration.loadFollowTermsData(TERMS_FILENAME);
        logger.info(terms.toString());

        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // Twitter client authentication
        String [] authData = ClientConfiguration.loadAuthData(CONFIG_FILENAME);
        Authentication hosebirdAuth = new OAuth1(authData[0], authData[1], authData[2], authData[3]);

        // Client build & connect
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts) // where to seek
                .authentication(hosebirdAuth) // auth
                .endpoint(hosebirdEndpoint) // what to track
                .processor(new StringDelimitedProcessor(msgQueue)); // where to process

        return builder.build();
    }
}

class ClientConfiguration {

    private static Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    // Load 4 pieces of data needed to authenticate the client
    static String[] loadAuthData(String filename) {
        String[] authData = new String[4];
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
        return authData;
    }

    // Load a file and return each line's text trimmed in a list
    static List<Long> loadFollowPeopleData(String filename) {
        ArrayList<Long> loadFollowPeopleData = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = br.readLine()) != null) {
                try {
                    Long followId = Long.parseLong(line.trim());
                    loadFollowPeopleData.add(followId);
                }
                catch (NumberFormatException e){
                    logger.info("Invalid ID to follow provided");
                }
            }
        }
        catch (IOException e){
            e.printStackTrace();
        }
        return loadFollowPeopleData;
    }

    // Load a file and return each line's text trimmed in a list
    static List<String> loadFollowTermsData(String filename) {
        ArrayList<String> loadFollowTermsData = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = br.readLine()) != null) {
                loadFollowTermsData.add(line.trim());
            }
        }
        catch (IOException e){
            e.printStackTrace();
        }
        return loadFollowTermsData;
    }
}
