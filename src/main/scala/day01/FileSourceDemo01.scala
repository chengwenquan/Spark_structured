package day01

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructType}

object FileSourceDemo01 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("ReadFromFile")
      .getOrCreate()

    // 定义 Schema, 用于指定列名以及列中的数据类型
    val userSchema: StructType = new StructType().add("name", StringType).add("age", LongType).add("sex", StringType)
    val user: DataFrame = spark.readStream
      .format("csv")
      .schema(userSchema)
      .load("D:\\tmpFile\\structured")  // 必须是目录, 不能是文件名
      .groupBy("sex").sum("age")


     user.writeStream
      .outputMode("update")
      .trigger(Trigger.ProcessingTime(1000)) // 触发器 数字表示毫秒值. 0 表示立即处理
      .format("console")
      .start()
      .awaitTermination()

  }

}
