package HaoXin.LearFlink.Streamin.WriteDataToLocalFIle

import java.time.{LocalDate, LocalTime}

import org.apache.commons.lang.RandomStringUtils
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random

class mySource extends RichParallelSourceFunction[(String,Int,Int)]{
  var isRunning=true
  var num=0
  override def run(sourceContext: SourceFunction.SourceContext[(String, Int, Int)]): Unit = {
    while (isRunning){
      val time=s"${LocalDate.now()} ${LocalTime.now().getHour}:${LocalTime.now().getMinute}:${LocalTime.now().getSecond}"
      sourceContext.collect(time,Random.nextInt(100),Random.nextInt(50))
      num+=1
      if (num>=900000000){
        isRunning=false
      }
    }
  }

  override def cancel(): Unit = {
    isRunning=false
  }
}
