package com.petro.prydorozhnyi.kafka.demo;

import java.util.Objects;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.petro.prydorozhnyi.kafka.ApplicationConstants;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Producer {

    /**
     * Producer. https://kafka.apache.org/documentation/#producerconfigs
     *
     * @param args - application arguments
     */
    public static void main(String[] args) {
        // producer props
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ApplicationConstants.LOCAL_KAFKA_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        //producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //produce records
        for (int i = 0; i < 10; i++) {
            String key = "id_" + i;
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", key,
                    "hello, guys " + i);
            // send data
            producer.send(record, (recordMetadata, e) -> {
                if (Objects.isNull(e)) {
                    log.info("Received metadata. Topic: {}. Partition: {}. Offsets: {}. Timestamp: {}.",
                            recordMetadata.topic(),
                            recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                } else {
                    log.error("Error during producing", e);
                }
            });
        }
        //flush and close producer
        producer.close();
    }

}
