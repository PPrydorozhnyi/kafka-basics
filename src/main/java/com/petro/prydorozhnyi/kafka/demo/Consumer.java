package com.petro.prydorozhnyi.kafka.demo;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

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

        //consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ApplicationConstants.LOCAL_KAFKA_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, ApplicationConstants.CONSUMER_GROUP_ID);
        //earliest, latest, none
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribe to topics
        consumer.subscribe(Collections.singleton("first_topic"));

        //poll new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            records.forEach( record ->
                log.info("Key: {}. Value: {}. Partition: {}. Offset: {}", record.key(), record.value(),
                        record.partition(), record.offset()));
        }


    }

}
