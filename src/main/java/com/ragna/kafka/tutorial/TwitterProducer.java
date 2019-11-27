package com.ragna.kafka.tutorial;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    // Auth data
    private String consumerKey = "sbfRsXNRsGtVQLHKxJ90HtmCh";
    private String consumerSecret = "WKNOxA0cEQjJC0A7LVMM39cDyS93qJWiw8FIMWLI4TLIsNCh6J";
    private String token = "1581135692-ru99UByfK0SYE21xtZe8H1UZ4wJoF8yqA7Td5X1";
    private String tokenSecret = "V0GUdFJTPGbrAzl9n9fYiF6Zbl0soEfRfvz64m1568Z3W";

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    private TwitterProducer() {

    }

    private void run() {
        // Create Twitter Client

        /* Blocking queues: where the data fetched will be allocated */
        logger.info("Setting up Twitter client");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
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

    public Client createTwitterClients (BlockingQueue<String> msgQueue) {

        // Client setup

        /* Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings of people or terms
        List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("kafka", "tesla");
        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // Twitter client authentication
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

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
