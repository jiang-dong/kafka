package com.jd.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * 消费订单数据--- javaben.tojson
 */
public class OrderConsumer {
    public static void main(String[] args) {
// 1\连接集群
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop01:9092");
        props.put("group.id", "test");

//以下两行代码 ---消费者自动提交offset值
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
//		 2、发送数据 发送数据需要，订阅下要消费的topic。order
        kafkaConsumer.subscribe(Arrays.asList("test"));

        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);
            // jdk queue offer插入、poll获取元素。 blockingqueue put插入原生， take获取元素
            for (ConsumerRecord<String, String> record : consumerRecords) {
               // System.out.println("消费的数据为：" + record.value());
                System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
            }
        }
    }
}

