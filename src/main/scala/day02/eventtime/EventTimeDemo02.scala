package day02.eventtime

import org.apache.spark.sql.SparkSession

/**
  * 使用window对时间戳进行分段查询
  */
object EventTimeDemo02 {
  def main(args: Array[String]): Unit = {
    //创建sparkSession
    val spark: SparkSession = SparkSession.builder().appName("windowDemo").master("local[2]").getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    //获取数据源
    val lines = spark.readStream
      .format("socket")
      .option("host","hadoop200")
      .option("port",9999)
      .load()

    //处理数据，将时间戳和数据分开  数据格式：2019-08-14 10:55:00,dog
    val words = lines.as[String].map(e=>{
      val splits = e.split(",")
      (splits(0),splits(1))
    }).flatMap({case (ts,words)=>words.split(" ").map((ts,_))})
      .toDF("ts","word")
      .groupBy(window($"ts","10 minutes","5 minutes"),$"word")
      .count()

    words.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate","false")
      .start()
      .awaitTermination()




  }
}
