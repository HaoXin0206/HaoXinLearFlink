package Kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @ClassName: ProperticeDemo
 * @Author: 郝鑫
 * @Data: 2019/7/5/22:10
 * @Descripition:
 */
public class ProperticeDemo {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","192.168.192.128:9092,192.168.192.128:9093,192.168.192.128:9094");
        properties.put("request.required.acks","0");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        Producer<String, String> stringStringProducer = new KafkaProducer<String, String>(properties);
        for (int i = 1; i < 10; i++) {
            stringStringProducer.send(new ProducerRecord<String, String>("haoxin01",Integer.toString(i),Integer.toString(i)));
            Thread.sleep(1000);
        }
        stringStringProducer.close();
    }
}
