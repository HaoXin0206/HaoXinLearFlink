package HaoXin.LearFlink.Streamin.CustomSource

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object ReadMySource {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    val env = StreamExecutionEnvironment.createLocalEnvironment(1)

    val datas: DataStream[String] = env.addSource(new ParaSourceFunction)

    val data = env.addSource(new ParaSourceFunction)
        .map(a=>{
          val data=a.split(" ")
          Info(data(0),data(1),data(2).toLong)
        })

    data.addSink(new JdbcWrite)
//    data.addSink(new WriteDataToHdfs)
    data.print()


    env.execute("")

  }
  case class Info(time:String,name:String,phon:Long)
}
