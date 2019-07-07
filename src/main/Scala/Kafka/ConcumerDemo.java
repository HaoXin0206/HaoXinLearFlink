package Kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

/**
 * @ClassName: ConcumerDemo
 * @Author: 郝鑫
 * @Data: 2019/7/5/22:11
 * @Descripition:
 */
public class ConcumerDemo {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","192.168.192.128:9092,192.168.192.128:9093,192.168.192.128:9094");
        properties.put("group.id", "test-1");
        properties.put("auto.commit.interval.ms", "2000");
        properties.put("request.required.acks","0");
        properties.put("auto.commit.offset","false");
        properties.put("auto.offset.reset","latest");
//        properties.put("max.partition.fetch.bytes","102400");
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        final KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        //指定消费者订阅的topic
        kafkaConsumer.subscribe(Arrays.asList("haoxin01"));

        while (true){

            //获取数据
            ConsumerRecords<String, String> poll = kafkaConsumer.poll(Duration.ofSeconds(30));

            for (ConsumerRecord<String, String> record :poll) {

//                数据消费
                System.out.printf("partition= %d, offset = %d, timestamp=%s, key = %s, value = %s%n",record.partition(), record.offset(),record.timestamp(), record.key(), record.value());

                //手动提交偏移量
                HashMap<TopicPartition, OffsetAndMetadata> top = new HashMap<>();
                TopicPartition topicPartition = new TopicPartition("haoxin01", record.partition());
                OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(record.offset());
                top.put(topicPartition,offsetAndMetadata);
                kafkaConsumer.commitSync(top);
            }




            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


        }
    }
}
