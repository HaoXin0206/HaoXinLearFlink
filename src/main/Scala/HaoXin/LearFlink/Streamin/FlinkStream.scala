package HaoXin.LearFlink.Streamin

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object FlinkStream {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment



    env.socketTextStream("haoxin01",9000)
        .map(a=>{
          tt(a)
          a
        })




    env.execute()

  }

  def  tt(data:String): Unit ={
    println(data)
  }

}
