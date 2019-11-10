package HaoXin.LearFlink.Kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @ClassName: ProducerDemo
 * @Author: 郝鑫
 * @Data: 2019/8/4/10:50
 * @Descripition:
 */
public class ProducerDemo {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","192.168.65.130:9092,192.168.65.130:9093,192.168.65.130:9094");
        properties.put("request.required.acks","2");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        Producer<String, String> stringStringProducer = new KafkaProducer<String, String>(properties);
        for (int i = 1; i < 100000; i++) {

            stringStringProducer.send(new ProducerRecord<String, String>("haoxin01",Integer.toString(i),
                    Integer.toString(i)));
        }
        stringStringProducer.close();
    }
}
