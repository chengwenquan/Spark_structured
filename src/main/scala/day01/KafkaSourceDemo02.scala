package day01

import org.apache.spark.sql.SparkSession

object KafkaSourceDemo02 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("demo02")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "hadoop200:9092,hadoop201:9092,hadoop202:9092")
      .option("subscribe", "first")
      .load()

      df.selectExpr( "CAST(value AS STRING)")
      .as[String].flatMap(_.split(" ")).groupBy("value").count()
      .write.format("console").save()



  }
}
