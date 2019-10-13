package day01

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object WordCountDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("wordCount").master("local[2]").getOrCreate()

    val lines: DataFrame = spark.readStream.format("socket")
      .option("host", "hadoop200")
      .option("port", 9999)
      .load()

   // lines.writeStream.outputMode("append").format("console").start().awaitTermination()
    import spark.implicits._
    lines.as[String].flatMap(_.split(" "))
      .groupBy("value")
      .count()
      .writeStream.outputMode("update")
      .format("console")
      .start()
      .awaitTermination()
  }
}

object WordCountDemo01{
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkSession. 因为 ss 是基于 spark sql 引擎, 所以需要先创建 SparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("WordCount1")
      .getOrCreate()
    import spark.implicits._
    // 2. 从数据源(socket)中加载数据.
    val lines: DataFrame = spark.readStream    //lines DataFrame是“输入表”
      .format("socket") // 设置数据源
      .option("host", "hadoop201")
      .option("port", 9999)
      .load

    // 3. 把每行数据切割成单词
    val words: Dataset[String] = lines.as[String].flatMap(_.split("\\W"))

    // 4. 计算 word count                      //wordCounts DataFrame 是“结果表”
    val wordCounts: DataFrame = words.groupBy("value").count()

    // 5. 启动查询, 把结果打印到控制台
    val query: StreamingQuery = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start
    query.awaitTermination()

    spark.stop()

  }
}