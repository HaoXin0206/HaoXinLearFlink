package HaoXin.LearFlink.Streamin.FlinkHdfs

import java.time.LocalDate

import org.apache.flink.streaming.api.scala._


object WriteDataToHDFS {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.addSource(new MySource)
      .map(a=>{
        (s"${a.name}\t${a.age}\t${a.sex}\t${a.address}\t${a.coreid}\t${a.money}")
      })
      .writeAsText(s"hdfs://haoxin01:8020/user/haoxin/Data/UserInfo/${LocalDate.now()}")

    env.execute("HaoXin Learn Flink Demo")

  }

  case class userinfo(name:String,age:Int,sex:String,address:String,coreid:String,money:Int)

}
