package HaoXin.LearFlink.Core

import java.io.{File, FileInputStream}
import java.util

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

object DiscFile {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val filepath="D:\\data\\20191015.txt"
    env.registerCachedFile(filepath,"dsc_file")


    env.fromCollection(Seq(1,2,3,4,5,6,67,3))
      .map(new RichMapFunction[Int,String] {
        var file: File=null
        override def open(parameters: Configuration): Unit = {
          file = getRuntimeContext.getDistributedCache.getFile("dsc_file")

          val list: util.List[String] = FileUtils.readLines(file,"UTF-8")

          import scala.collection.JavaConversions._
          for (a <- list ){
            println(a)
          }
        }

        override def map(in: Int): String = {
          in.toString
        }
      })
      .print()

  }

}
