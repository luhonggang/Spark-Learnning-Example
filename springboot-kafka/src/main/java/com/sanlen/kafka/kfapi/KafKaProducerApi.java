package com.sanlen.kafka.kfapi;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author liuzh
 * @since 2017/6/25.
 */
public class KafKaProducerApi {

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "www.hadoop.com:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 10; i++) {
            TimeUnit.MILLISECONDS.sleep(100);
            producer.send(new ProducerRecord<String, String>("test", Integer.toString(i), Integer.toString(i)));
            producer.send(new ProducerRecord<String, String>("test02", Integer.toString(i), Integer.toString(i)));
        }
        producer.close();
    }
}
