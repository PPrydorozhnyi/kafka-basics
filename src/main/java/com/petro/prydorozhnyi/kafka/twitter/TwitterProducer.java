package com.petro.prydorozhnyi.kafka.twitter;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import com.petro.prydorozhnyi.kafka.ApplicationConstants;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {

    public TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
    }

    public void createTwitterClient() {
        LinkedBlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100_000);

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
    }

}
