package HaoXin.LearFlink.Streamin.Times

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object TimeWater {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.addSource(new DataSource)
      .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[(String, Int)] {
        val maxtime = 1
        var curTime: Int = _

        override def checkAndGetNextWatermark(t: (String, Int), l: Long): Watermark = {
          new Watermark(curTime - maxtime)
        }

        override def extractTimestamp(t: (String, Int), l: Long): Long = {
          curTime = Math.max(t._2, maxtime)
          curTime
        }
      })
      .keyBy(0)
      .timeWindow(Time.seconds(4))
      .sum(1)
      .print()

    env.execute("")
  }

}
