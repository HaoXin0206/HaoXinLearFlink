package HaoXin.LearFlink.Streamin.WriteDataToLocalFIle

import org.apache.flink.streaming.api.scala._

object Main {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.addSource(new mySource)
      .writeAsText("D:\\TestData\\20190729")

    env.execute()
  }

}
