package day02.eventtime

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession

/**
  * 使用window对时间戳进行分段查询
  */
object EventTimeDemo {
  def main(args: Array[String]): Unit = {
    //创建sparkSession
    val spark: SparkSession = SparkSession.builder().appName("windowDemo").master("local[2]").getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    //获取数据源
    val lines = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",9999)
      .option("includeTimestamp",true)// 给产生的数据自动添加时间戳
      .load()

    //处理数据
    val words = lines.as[(String,Timestamp)]
      .flatMap({case (str,time) => str.split(" ").map((_,time))
      }).toDF("word","timestamp")

    val wordCounts = words.groupBy(
      window($"timestamp", "10 minutes", "5 minutes"),
      $"word")
      .count().orderBy($"window")

    //输出数据到控制台
    wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")// 不截断.为了在控制台能看到完整信息, 最好设置为 false
      .start()
      .awaitTermination()
  }
}
