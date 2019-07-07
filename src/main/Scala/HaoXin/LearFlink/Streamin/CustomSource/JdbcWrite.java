package HaoXin.LearFlink.Streamin.CustomSource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @ClassName: JdbcWrite
 * @Author: 郝鑫
 * @Data: 2019/7/6/7:51
 * @Descripition:
 */
public class JdbcWrite extends RichSinkFunction<ReadMySource.Info> {
    Connection connection;
    PreparedStatement preparedStatement;
    Option option=new Option();
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName(option.mysqlDriver);
        connection = DriverManager.getConnection(option.mysqlUrl, option.mysqlUserName, option.mysqlPasswd);
        preparedStatement = connection.prepareStatement(option.mysqlInsertSQL);
    }

    @Override
    public void invoke(ReadMySource.Info value, Context context) throws Exception {
        preparedStatement.setString(1,value.time());
        preparedStatement.setString(2,value.name());
        preparedStatement.setString(3,value.phon()+"");
        preparedStatement.execute();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (null!=preparedStatement){
            preparedStatement.close();
        }
        if (null!=connection){
            connection.close();
        }
    }
}
