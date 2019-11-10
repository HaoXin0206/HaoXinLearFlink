package Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @ClassName: HbaseDemo
 * @Author: 郝鑫
 * @Data: 2019/8/8/19:48
 * @Descripition:
 */
public class HbaseDemo {
    Configuration configuration;
    Connection connection;
    public static void main(String[] args) {

    }


    public void getTable() throws IOException {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "haoxin01");
        configuration.set("hbase.zookeeper.property.clientPort","2184");
        connection = ConnectionFactory.createConnection();

    }

}
