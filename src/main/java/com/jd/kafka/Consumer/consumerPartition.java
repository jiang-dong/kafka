package com.jd.kafka.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Properties;

public class consumerPartition  {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop01:9092");
        props.put("group.id", "test_group");//消费组
        props.put("enable.auto.commit", "false"); //禁用自动提交offset，后期我们手动提交offset
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);

        //通过consumer订阅某一topic,进行消费,会消费topic里面分区的数据
        //kafkaConsumer.subscribe(Arrays.asList());  //订阅test这个topic

        //通过调用assign方法实现消费mypartition
        TopicPartition mypartition0 = new TopicPartition("mypartition", 0);
        TopicPartition mypartition1 = new TopicPartition("mypartition", 1);
        //订阅我们某个topic 里面指定分区的数据进行消费
        kafkaConsumer.assign(Arrays.asList(mypartition0,mypartition1));
        int i =0;
        while (true) {

            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(1000);
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                i++;
                System.out.println("数据值为"+consumerRecord.value()+"数据的offset"+consumerRecord.offset());
            }
        }
    }

}
