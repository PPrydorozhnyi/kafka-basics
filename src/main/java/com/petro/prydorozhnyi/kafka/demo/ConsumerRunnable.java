package com.petro.prydorozhnyi.kafka.demo;

import java.time.Duration;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConsumerRunnable implements Runnable {

    private final CountDownLatch latch;
    KafkaConsumer<String, String> consumer;

    public ConsumerRunnable(String bootstrapServer,
                            String groupId,
                            Set<String> topics,
                            CountDownLatch latch) {
        this.latch = latch;

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //earliest, latest, none
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(topics);
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record ->
                        log.info("Key: {}. Value: {}. Partition: {}. Offset: {}", record.key(), record.value(),
                                record.partition(), record.offset()));
            }
        } catch (WakeupException e) {
            log.info("Received shutdown signal");
        } finally {
            consumer.close();
            //tels to spot
            latch.countDown();
        }
    }

    /**
     * Aborts poll.
     */
    public void shutdown() {
        consumer.wakeup();
    }

}
