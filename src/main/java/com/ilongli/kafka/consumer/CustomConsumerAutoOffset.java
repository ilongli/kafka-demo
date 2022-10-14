package com.ilongli.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by ilongli on 2022/9/28.
 */
public class CustomConsumerAutoOffset {

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

        // 自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        // 提交时间间隔
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);

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
