package HaoXin.LearFlink.Streamin.CustomSource;

import scala.Serializable;

import java.io.SerializablePermission;

/**
 * @ClassName: HaoXin.LearFlink.Streamin.CustomSource.Option
 * @Author: 郝鑫
 * @Data: 2019/7/6/7:57
 * @Descripition:
 */
public  class Option implements Serializable {
    public final String mysqlDriver="com.mysql.jdbc.Driver";
    public final String mysqlUrl="jdbc:mysql://localhost:3306/haoxin";
    public final String mysqlUserName="root";
    public final String mysqlPasswd="123456";
    public final String mysqlInsertSQL="insert into UserInfo(time,name,phone) values(?,?,?)";

}
