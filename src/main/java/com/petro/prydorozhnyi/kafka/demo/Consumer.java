package com.petro.prydorozhnyi.kafka.demo;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import com.petro.prydorozhnyi.kafka.ApplicationConstants;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Consumer {

    /**
     * Consumer simple demo.
     * https://kafka.apache.org/documentation/#consumerconfigs
     * @param args - application variables.
     */
    public static void main(String[] args) {
        CountDownLatch countDownLatch = new CountDownLatch(1);

        ConsumerRunnable consumerRunnable = new ConsumerRunnable(ApplicationConstants.LOCAL_KAFKA_SERVER,
                ApplicationConstants.CONSUMER_GROUP_ID, Collections.singleton("first_topic"), countDownLatch);

        Thread thread = new Thread(consumerRunnable);

        thread.start();

        //shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Caight shutdown hook");
            consumerRunnable.shutdown();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                log.error("Application going to stop", e);
            }
            log.info("Exited");
        }));

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error("Application interrupted", e);
        }
    }

}
