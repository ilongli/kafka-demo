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
import java.util.Set;

/**
 * 指定offset开始消费
 * Created by ilongli on 2022/9/28.
 */
public class CustomConsumerSeek {

    public static void main(String[] args) {

        // 0.配置
        Properties properties = new Properties();

        // 连接集群
//        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "sc1:9091,sc2:9092,sc3:9093");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "sc1:9091");
        // 指定对应的key和value的序列化类型
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

//        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // 配置消费者组id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test2");

        // 1.创建一个消费者 "", "hello"
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // 2.订阅主题(first)
        ArrayList<String> topics = new ArrayList<>();
        topics.add("second");
        kafkaConsumer.subscribe(topics);

        // 指定位置进行消费
        Set<TopicPartition> assignments = kafkaConsumer.assignment();

        // 保证分区分配方案已经制定完毕
        while (assignments.size() == 0) {
            kafkaConsumer.poll(Duration.ofSeconds(1));
            assignments = kafkaConsumer.assignment();
        }

        kafkaConsumer.seekToBeginning(assignments);

/*        for (TopicPartition assignment : assignments) {
            // 指定消费的offset
            kafkaConsumer.seek(assignment, 3);
        }*/

        // 3.消费数据
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }

        }

    }

}
