package day02.watermark

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable

/**
  * 水印测试
  */
object WaterMarkDemo01 {
  def main(args: Array[String]): Unit = {
    //创建sparkSession
    val spark: SparkSession = SparkSession.builder().appName("waterMark").master("local[*]").getOrCreate()
    //获取数据
    val dataFrame: DataFrame = spark.readStream.format("socket").option("host","localhost").option("port",9999).load()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    //对数据进行处理 数据格式:2019-08-14 10:55:00,dog cat
    val df = dataFrame.as[String].flatMap(e => {
      val lines: mutable.ArrayOps[String] = e.split(",")
      lines(1).split(" ").map((Timestamp.valueOf(lines(0)), _))
    }).toDF("ts","word")

    //添加水印 参数 1: event-time 所在列的列名 参数 2: 延迟时间的上限.
    val result: DataFrame = df.withWatermark("ts", "2 minutes")
      .groupBy(window($"ts", "10 minutes", "2 minutes"), $"word")
      .count()

    result.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate","false")
      .start()
      .awaitTermination()


  }
}
