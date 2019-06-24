package HaoXin.LearFlink.Core

import org.apache.flink.api.scala._

object FlinkCoreDemo {
  def main(args: Array[String]): Unit = {
    val env=ExecutionEnvironment.getExecutionEnvironment

    val tes=env.fromElements("-","?","\<","\>","\{","\}","\:","_","\.")

    env.readTextFile("D:\\code\\Flink\\pom.xml")
      .flatMap(a=>a.split(""))
      .map(a=>{
        
        a
      })
      .map(a=>(a.toUpperCase,1))
      .groupBy(0)
      .sum(1)
      .print()

    Thread.sleep(100000000000000000L)

  }

}
