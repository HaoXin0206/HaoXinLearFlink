package HaoXin.LearFlink.Streamin

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object FlinkStream {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment

    env.socketTextStream("haoxin01",9000)
      .flatMap(a=>a.split(""))
      .map(a=>(a,1))
      .keyBy(0)
      .timeWindow(Time.seconds(2),Time.seconds(2))
      .sum(1)
      .print()


    env.execute()

  }

}
