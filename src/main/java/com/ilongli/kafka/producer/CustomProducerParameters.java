package com.ilongli.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Created by ilongli on 2022/5/8.
 */
public class CustomProducerParameters {

    public static void main(String[] args) {

        // 0.配置
        Properties properties = new Properties();

        // 连接kafka集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.81:9092,192.168.1.82:9092,192.168.1.83:9092");

        // 序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 缓冲区大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        // 批次大小
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // linger.ms
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        // 压缩
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        // 1.创建生产者
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        // 2.发送数据
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("first", "ilongli" + i));
        }

        // 3.关闭资源
        kafkaProducer.close();
    }

}
