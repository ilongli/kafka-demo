package com.ilongli.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * 指定时间开始消费（消费指定时间之后的数据）
 * Created by ilongli on 2022/9/28.
 */
public class CustomConsumerSeekTime {

    public static void main(String[] args) {

        // 0.配置
        Properties properties = new Properties();

        // 连接集群
//        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "sc1:9091,sc2:9092,sc3:9093");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "sc1:9091");
        // 指定对应的key和value的序列化类型
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 配置消费者组id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");

        // 1.创建一个消费者 "", "hello"
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // 2.订阅主题(first)
        ArrayList<String> topics = new ArrayList<>();
        topics.add("first");
        kafkaConsumer.subscribe(topics);

        // 指定位置进行消费
        Set<TopicPartition> assignments = kafkaConsumer.assignment();

        // 保证分区分配方案已经制定完毕
        while (assignments.size() == 0) {
            kafkaConsumer.poll(Duration.ofSeconds(1));
            assignments = kafkaConsumer.assignment();
        }

        // 希望把时间转换为对应的offset
        HashMap<TopicPartition, Long> topicPartitionLongHashMap = new HashMap<>();

        // 一天前的数据
        long time = System.currentTimeMillis() - 1 * 24 * 3600 * 1000;

        // 封装对应集合
        for (TopicPartition assignment : assignments) {
            topicPartitionLongHashMap.put(assignment, time);
        }

        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap = kafkaConsumer.offsetsForTimes(topicPartitionLongHashMap);

        for (TopicPartition assignment : assignments) {
            // 指定消费的offset
            long offset = topicPartitionOffsetAndTimestampMap.get(assignment).offset();
            System.out.println(offset);
            kafkaConsumer.seek(assignment, offset);
        }

        // 3.消费数据
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }

        }

    }

}
