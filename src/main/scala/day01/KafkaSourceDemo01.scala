package day01

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}


object KafkaSourceDemo01 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("demo02")
      .master("local[2]")
      .getOrCreate()
    val df: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "hadoop200:9092,hadoop201:9092,hadoop202:9092")
      .option("subscribe", "first").load()

    df.writeStream
      .outputMode("update")
      .format("console")
      .trigger(Trigger.Continuous(1000))
      .start
      .awaitTermination()
  }
}
