package com.petro.prydorozhnyi.kafka.twitter;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.petro.prydorozhnyi.kafka.ApplicationConstants;
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
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TwitterProducer {

    public TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {

        LinkedBlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1_000);

        Client client = createTwitterClient(msgQueue);

        //attempts to establish the connection
        client.connect();

        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("Cannot take message", e);
                client.stop();
            }

            if (msg != null) {
                log.info("Message is {}", msg);
            }
        }

        log.info("End of run");
    }

    public BasicClient createTwitterClient(LinkedBlockingQueue<String> msgQueue) {

        log.info("Setting up client");

        // Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);

        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = List.of("kafka");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(ApplicationConstants.CONSUMER_KEY,
                ApplicationConstants.CONSUMER_SECRET, ApplicationConstants.TOKEN,
                ApplicationConstants.SECRET);


        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        log.info("Client has built");

        return builder.build();
    }

}
