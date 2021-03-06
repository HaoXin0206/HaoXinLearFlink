package HaoXin.LearFlink.Core

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.runtime.rest.messages.job.metrics.MetricsAggregationParameter._

object ReadHdfsFile {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val path = "/haoxin/data"
    val data = env.readTextFile(s"hdfs://haoxin01:8020${path}")
      .flatMap(_.split(""))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .sortPartition(1,Order.ASCENDING)
      .setParallelism(1)
      .print()

  }

}
