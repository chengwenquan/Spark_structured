package day02.watermark

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 流数据去重
  */
object DropduplicateDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("duplicatedemo").master("local[*]").getOrCreate()

    val dataFrame: DataFrame = spark.readStream.format("socket").option("host","localhost").option("port",9999).load()
// 数据格式：
//    1,2019-09-14 11:50:00,dog
//    2,2019-09-14 11:51:00,dog
//    1,2019-09-14 11:50:00,dog
//    3,2019-09-14 11:53:00,dog
//    1,2019-09-14 11:50:00,dog
//    4,2019-09-14 11:45:00,dog
    import spark.implicits._
    val words: DataFrame = dataFrame.as[String].map(e => {
      val splits: Array[String] = e.split(",")
      (splits(0), Timestamp.valueOf(splits(1)), splits(2))
    }).toDF("id", "timestamp", "word")

    words.withWatermark("timestamp","2 minutes").dropDuplicates("id")
      .writeStream.outputMode("update")
      .format("console")
      .option("truncate","false")
      .start()
      .awaitTermination()
  }
}

//+---+-------------------+----+
//|id |timestamp          |word|
//+---+-------------------+----+
//|3  |2019-09-14 11:53:00|dog |
//|1  |2019-09-14 11:50:00|dog |
//|4  |2019-09-14 11:45:00|dog |
//|2  |2019-09-14 11:51:00|dog |
//+---+-------------------+----+