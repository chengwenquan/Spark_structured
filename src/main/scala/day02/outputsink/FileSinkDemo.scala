package day02.outputsink

import org.apache.spark.sql.SparkSession

object FileSinkDemo {

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

    //.option("path", "./filesink") // 输出目录
    //            .option("checkpointLocation", "./ck1")  // 必须指定 checkpoint 目录
    dataFrame.writeStream
      .outputMode("append")
      .format("json")
      .option("path","D:\\tmpFile\\jsonFile")
      .option("checkpointLocation","./ck1")
      .start()
      .awaitTermination()
  }
}
