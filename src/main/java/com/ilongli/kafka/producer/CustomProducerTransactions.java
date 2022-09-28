package com.ilongli.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


/**
 * Created by ilongli on 2022/4/23.
 */
public class CustomProducerTransactions {

    public static void main(String[] args) {

        // 0.配置
        Properties properties = new Properties();

        // 连接集群
//        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.81:9092,192.168.1.82:9092,192.168.1.83:9092");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "sc1:9091");
        // 指定对应的key和value的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 指定事务id（全局唯一）
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactional_id_02");

        // 1.创建kafka生产者对象
        // "" hello
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 初始化事务
        producer.initTransactions();

        // 开启事务
        producer.beginTransaction();

        try {
            // 2.发送数据
            for (int i = 0; i < 5; i++) {
                producer.send(new ProducerRecord<>("first", "ilongli" + i));
            }

//            int i = 1 / 0;

            // 提交事务
            producer.commitTransaction();
        } catch (Exception e) {
            // 放弃事务（事务回滚）
            producer.abortTransaction();
        } finally {
            // 3.关闭资源
            producer.close();
        }
    }

}
