package com.pluralsight.kafka.producer;

import com.pluralsight.kafka.producer.model.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

import static java.lang.Thread.sleep;

@Slf4j
public class Main {

    public static void main(String[] args) throws InterruptedException {

        EventGenerator eventGenerator = new EventGenerator();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9093,localhost:9094");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        for(int i = 1; i <= 5; i++) {
            log.info("Generating event #" + i);

            Event event = eventGenerator.generateEvent();

            String key = extractKey(event);
            String value = extractValue(event);

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("user-tracking", key, value);

            log.info("Producing to Kafka the record: " + key + ":" + value);
            producer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e== null) {
                        log.info("Successfully received the details as: \n" +
                                "\tTopic: " + recordMetadata.topic() + "\n" +
                                "\tPartition: " + recordMetadata.partition() + "\n" +
                                "\tOffset: " + recordMetadata.offset() + "\n" +
                                "\tTimestamp: " + recordMetadata.timestamp());
                    } else {
                        log.error("Can't produce,getting error", e);
                    }
                }
            });

            sleep(1000);
        }

        producer.close();
    }

    private static String extractKey(Event event) {
        return event.getUser().getUserId().toString();
    }

    private static String extractValue(Event event) {
        return String.format("%s,%s,%s", event.getProduct().getType(), event.getProduct().getColor(), event.getProduct().getDesignType());
    }


}
