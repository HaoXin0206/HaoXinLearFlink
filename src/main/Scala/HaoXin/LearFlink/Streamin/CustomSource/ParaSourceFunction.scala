package HaoXin.LearFlink.Streamin.CustomSource

import java.text.DecimalFormat
import java.time.{LocalDate, LocalDateTime, LocalTime}

import org.apache.commons.lang.RandomStringUtils
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, RichParallelSourceFunction, SourceFunction}

import scala.util.Random


class ParaSourceFunction extends RichParallelSourceFunction[String] {
  var isRunnning = true
  var num = 100
  private val format = new DecimalFormat("00")

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    while (isRunnning) {
      Thread.sleep(200)
      val time = s"${LocalDate.now()}\t${format.format(LocalTime.now().getHour)}:${format.format(LocalTime.now()
        .getMinute)}:${LocalTime.now().getSecond}"
      val name = s"${RandomStringUtils.random(1,"QWERTYUIOPASDFGHJKLZXCVBNM")}${RandomStringUtils.random(4,
        "qwertyuiopasdfghjklzxcvbnm")}"
      val phon = s"1${Random.nextInt(5)}${Random.nextInt(9)}${Random.nextInt(9)}${Random.nextInt(9)}${Random.nextInt(9)}${Random.nextInt(9)}${Random.nextInt(9)}${Random.nextInt(9)}${Random.nextInt(9)}${Random.nextInt(9)}${Random.nextInt(9)}${Random.nextInt(9)}"
      ctx.collect(s"${time} ${name} ${phon}")
    }
  }

  override def cancel(): Unit = {
    isRunnning = true
  }
}

