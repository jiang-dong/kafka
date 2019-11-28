package com.jd.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 订单的生产者代码，
 */
public class OrderProducer {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        /* 1、连接集群，通过配置文件的方式
         * 2、发送数据-topic:order，value
         */
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop01:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100; i++) {
            // 发送数据 ,需要一个producerRecord对象,最少参数
            Future<RecordMetadata> test = kafkaProducer.send(new ProducerRecord<String, String>("test", "订单信息！" + i));

            Thread.sleep(100);
            System.out.println(test.get().offset());
        }
    }
}

