package HaoXin.LearFlink.Streamin.FlinkHdfs

import java.io.{BufferedWriter, File}
import java.util.stream.IntStream

import org.apache.commons.io.FileUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.types.Row


class MySink extends RichSinkFunction[Row]{

  var file :File=_
  val path="D:\\data\\mysqlData\\hx_01.sql"
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    file = new File(path)
  }


  override def invoke(value: Row, context: SinkFunction.Context[_])
  : Unit = {
    val data: String =s"INSERT INTO `hx_01` VALUES ('${value.getField(0)}','${value.getField(1)}',${value.getField(2)}," +
      s"${value.getField(3)},'${value.getField(4)}',${value.getField(5)},'${value.getField(6)}');"
    println(data)
    FileUtils.writeStringToFile(file,data+"\n","UTF-8",true)

  }

  override def close(): Unit = {
    super.close()
  }


}
