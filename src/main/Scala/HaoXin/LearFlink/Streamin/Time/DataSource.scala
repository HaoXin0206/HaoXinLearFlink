package HaoXin.LearFlink.Streamin.Time

import org.apache.commons.lang.RandomStringUtils
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random

class DataSource extends RichParallelSourceFunction[(String,Int)]{
  var isRunning=true
  var flag=1
  override def run(sc: SourceFunction.SourceContext[(String, Int)]): Unit = {
    while (isRunning){
      val str = RandomStringUtils.random(1,"abcdefg")
      val i = Random.nextInt(10)
      sc.collect((str,i))
      println(s"${flag}==>"+(str,i))
      flag+=1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning=false
  }
}
