package HaoXin.LearFlink.Streamin


import org.apache.commons.lang.RandomStringUtils
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

object waterMarkDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    import org.apache.flink.streaming.api.scala._
    env.addSource(new RichParallelSourceFunction[(String, Long, Int)] {
      var isRuning = true

      override def run(sourceContext: SourceFunction.SourceContext[(String, Long, Int)]): Unit = {
        var num = 0
        while (isRuning) {
          val time = Random.nextInt(8) + 1
          val one = RandomStringUtils.random(1, "qwertyuiopasdfghjklzxcvbnm")
          val timeStamp = System.currentTimeMillis()
          val threeData = Random.nextInt(10) + 1
          val value = (one, timeStamp, threeData)
          sourceContext.collect(value)
          num += 1
          if (num >= 1000) isRuning = false else isRuning
          Thread.sleep(time * 1000)
        }
      }

      override def cancel(): Unit = {
        isRuning = false
      }
    })
      .map(a => a)
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long, Int)] {

        var currentMaxTimestamp = 0L
        var maxOutOfOrderness = 10000L // 最大允许的乱序时间是10s

        override def getCurrentWatermark: Watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness)

        override def extractTimestamp(t: (String, Long, Int), l: Long): Long = {
          val value = t._2
          currentMaxTimestamp = Math.max(currentMaxTimestamp, value)
          println(s"${Thread.currentThread().getName}===>${currentMaxTimestamp}===>${getCurrentWatermark.getTimestamp}")
          value
        }
      })

      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .apply(new WindowFunction[(String, Long, Int), (String, Int), Tuple, TimeWindow] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[(String, Long, Int)], out: Collector[(String, Int)]): Unit = {
          val keyStr = key.toString.replace("(", "").replace(")", "")
          var sum = 0
          val iterator = input.iterator
          while (iterator.hasNext) {
            val tuple = iterator.next()
            sum += tuple._3
          }

          out.collect((keyStr, sum))
        }
      })
      .print()


    env.execute("")

  }
}
