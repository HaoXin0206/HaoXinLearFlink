package HDFS

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object AppendHDFSFile {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","haoxin")
    val configuration = new Configuration()
    configuration.set("fs.defaultFS", "hdfs://haoxin01:8020")
    val fileSystem = FileSystem.get(configuration)

    val path = new Path("/haoxin/testData/20190707")
    if (!fileSystem.exists(path)){
      println("测试0")
      fileSystem.create(path).close()
    }
    println("测试1")
    val fSDataOutputStream = fileSystem.append(path)
    fSDataOutputStream.writeBytes("郝鑫，你真棒")
    fSDataOutputStream.writeBytes("是的，没错")
    println("测试2")
    fSDataOutputStream.flush()
    fSDataOutputStream.close()

  }

}
