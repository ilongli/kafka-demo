package com.ilongli.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Properties;


/**
 * Created by ilongli on 2022/4/23.
 */
public class CustomProducerCallback {

    public static void main(String[] args) throws InterruptedException {

        // 0.配置
        Properties properties = new Properties();

        // 连接集群
//        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.81:9092,192.168.1.82:9092,192.168.1.83:9092");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "sc1:9091");
        // 指定对应的key和value的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 1.创建kafka生产者对象
        // "" hello
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 2.发送数据
        for (int i = 0; i < 500; i++) {
            producer.send(new ProducerRecord<>("first", "ilongli" + i), new Callback() {

                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        System.out.println("主题: " + recordMetadata.topic() + " 分区: " + recordMetadata.partition());
                    }
                }
            });
            Thread.sleep(1);
        }
        // 3.关闭资源
        producer.close();
    }

}
