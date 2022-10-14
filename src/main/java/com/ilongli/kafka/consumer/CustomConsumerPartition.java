package com.ilongli.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @author ilongli
 * @date 2022/9/29 10:17
 */
public class CustomConsumerPartition {

    public static void main(String[] args) {


        // 0.配置
        Properties properties = new Properties();

        // 连接集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "sc1:9091");
        // 指定对应的key和value的序列化类型
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 配置消费者组id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");

        // 1.创建一个消费者 "", "hello"
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // 2.订阅主题对应的分区
        ArrayList<TopicPartition> topicPartition = new ArrayList<>();
        topicPartition.add(new TopicPartition("first", 0));
        kafkaConsumer.assign(topicPartition);

        // 3.消费数据
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }

        }


    }

}
