package day02.join

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 流数据join流数据
  */
object StreamToStream {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("stream").master("local[*]").getOrCreate()

    val stream1: DataFrame = spark.readStream.format("socket").option("host","localhost").option("port",10000).load()
    val stream2: DataFrame = spark.readStream.format("socket").option("host","localhost").option("port",20000).load()

    import spark.implicits._
    val stream1_result: DataFrame = stream1.as[String].map(e => {
      val splits: Array[String] = e.split(",")
      (splits(0), splits(1))
    }).toDF("id", "name")

    val stream2_result: DataFrame = stream2.as[String].map(e => {
      val splits: Array[String] = e.split(",")
      (splits(0), splits(1))
    }).toDF("id", "class")

    stream1_result.join(stream2_result,"id")
      .writeStream
      .outputMode("append")
      .format("console")
      .start()
      .awaitTermination()


  }
}

//输入数据：
//10000
//1,zhangsan
//2,lisi
//3,wangwu
//4,zhaoliu
//20000
//1,one
//2,two
//3,three

//+---+--------+-----+
//| id|    name|class|
//+---+--------+-----+
//|  3|  wangwu|three|
//|  1|zhangsan|  one|
//|  2|    lisi|  two|
//+---+--------+-----+