package day02

import org.apache.spark.sql.SparkSession

object TestNc {
  def main(args: Array[String]): Unit = {
    //创建sparkSession
    val spark: SparkSession = SparkSession.builder().appName("windowDemo").master("local[2]").getOrCreate()
    //获取数据源
    val dataFrame = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",9999)
      .option("includeTimestamp",true)
      .load()

    dataFrame.writeStream
      .outputMode("update")
      .format("console")
      .start()
      .awaitTermination()
  }

}
