package HaoXin.LearFlink.WriteData

import java.sql.Types

import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat
import org.apache.flink.streaming.api.scala._
import org.apache.flink.types.Row


object StreamData2MySql {
  val driver_name = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://localhost:3306/haoxin?\"+ \"useUnicode=true&characterEncoding=UTF8"
  val user_name = "root"
  val pass_wd = "123456"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val value = env.addSource(new MyDataSource).setParallelism(1)
      .map(a => {
        val row = new Row(7)
        row.setField(0, a._1)
        row.setField(1, a._2)
        row.setField(2, a._3)
        row.setField(3, a._4)
        row.setField(4, a._5)
        row.setField(5, a._6)
        row.setField(6, a._7)
        row
      })

    value.setParallelism(12)
        .writeUsingOutputFormat(JDBCOutputFormat.buildJDBCOutputFormat()
            .setDrivername(driver_name)
            .setDBUrl(url)
            .setUsername(user_name)
            .setPassword(pass_wd)
            .setQuery("INSERT INTo hx_01 values (?,?,?,?,?,?,?)")
            .setSqlTypes(Array[Int](Types.VARCHAR,Types.VARCHAR,Types.INTEGER,Types.VARCHAR,Types.VARCHAR,Types
              .INTEGER,Types.VARCHAR))
          .finish())



    env.execute("Data To MySql")

  }

}
