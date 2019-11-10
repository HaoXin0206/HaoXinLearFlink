package HaoXin.LearFlink

import java.sql.Types

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInfo}
import org.apache.flink.api.java.io.jdbc.{JDBCInputFormat, JDBCOutputFormat}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner

object FlinkDataSet {

  val db_driver_name = "com.mysql.jdbc.Driver"
  val db_url = "jdbc:mysql://localhost:3306"
  val db_user_name = "root"
  val db_pass_wd = "123456"

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val value = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
      .setDrivername(db_driver_name)
      .setDBUrl(db_url)
      .setUsername(db_user_name)
      .setPassword(db_pass_wd)
      .setQuery("select time,name,phone  from haoxin.userinfo")
      .setRowTypeInfo(new RowTypeInfo(
        BasicTypeInfo.DATE_TYPE_INFO,
        BasicTypeInfo.CHAR_TYPE_INFO,
        BasicTypeInfo.BIG_INT_TYPE_INFO
      ))
      .finish())


    value
      .output(JDBCOutputFormat.buildJDBCOutputFormat()
        .setDrivername(db_driver_name)
        .setDBUrl(db_url)
        .setUsername(db_user_name)
        .setPassword(db_pass_wd)
        .setQuery("inster into haoxin.userinfor (time,name,phone) values (?,?,?)")
        .setSqlTypes(Array[Int] (
          Types.DATE,
          Types.VARCHAR,
          Types.BIGINT
        ))
        .finish())

    new DateTimeBucketAssigner()

    StreamingFileSink.forBulkFormat(new Path(""),ParquetAvroWriters.forReflectRecord(info.getClass))
//      .withBucketAssigner()


  }

  case class info(time:String,name:String,phon:Int)

}
