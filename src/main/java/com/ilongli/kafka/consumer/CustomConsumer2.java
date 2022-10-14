package com.ilongli.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by ilongli on 2022/9/28.
 */
public class CustomConsumer2 {

    public static void main(String[] args) {

        // 0.配置
        Properties properties = new Properties();

        // 连接集群
//        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "sc1:9091,sc2:9092,sc3:9093");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "sc1:9091");
        // 指定对应的key和value的序列化类型
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 设置分区分配策略
        // https://kafka.apache.org/documentation/#consumerconfigs_partition.assignment.strategy
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RoundRobinAssignor");

        // 配置消费者组id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");


        // 1.创建一个消费者 "", "hello"
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // 2.订阅主题(first)
        ArrayList<String> topics = new ArrayList<>();
        topics.add("first");
        kafkaConsumer.subscribe(topics);

        // 3.消费数据
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }

        }


    }

}
