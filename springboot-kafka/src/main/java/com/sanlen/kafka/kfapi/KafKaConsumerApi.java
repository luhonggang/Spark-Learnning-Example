package com.sanlen.kafka.kfapi;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class KafKaConsumerApi {
    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "www.hadoop.com:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("test","test02"));
        while (true) {
            System.out.println("before");
            ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
            System.out.println("after");
            for (ConsumerRecord<String, String> record : records){
                System.out.printf("topic = %s, offset = %d, key = %s, value = %s%n", record.topic(), record.offset(), record.key(), record.value());
            }

        }

    }
}
