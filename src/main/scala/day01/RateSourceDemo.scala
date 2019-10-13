package day01

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 以固定的速率生成固定格式的数据, 用来测试 Structured Streaming 的性能.
  */
object RateSourceDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("rate").master("local[2]").getOrCreate()

    val df: DataFrame = spark.readStream.format("rate")
      .option("rowsPerSecond", 10) // 设置每秒产生的数据的条数, 默认是 1
      .option("rampUpTime", 1) // 设置多少秒到达指定速率 默认为 0
      .option("numPartitions", 2) //设置分区数  默认是 spark 的默认并行度
      .load()
    df.writeStream.outputMode("update")
      .format("console")
      .trigger(Trigger.Continuous(1000))
      .option("truncate",false)
      .start()
      .awaitTermination()

  }

}
