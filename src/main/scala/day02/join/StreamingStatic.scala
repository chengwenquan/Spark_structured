package day02.join

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable

/**
  * 流数据和静态数据的join
  */
object StreamingStatic {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("streamingStatic").master("local[*]").getOrCreate()

    import spark.implicits._
    //静态数据
    val staticDf: DataFrame = spark.sparkContext.parallelize(List((1,"one"),(2,"two"),(3,"three"))).toDF("id","class")
    //流数据
    val dataFrame = spark.readStream.format("socket").option("host","localhost").option("port",9999).load()
    //流式数格式：1，zhangsan
    val ds: Dataset[(String, String)] = dataFrame.map(e => {
      val splits: mutable.ArrayOps[String] = e.getString(0).split(",")
      (splits(0), splits(1))
    })
    val df = ds.toDF("id", "name")

//默认内连接
//    df.join(staticDf,"id")
//      .writeStream
//      .outputMode("update")
//      .format("console")
//      .start().awaitTermination()
    //左连接
    df.join(staticDf,Seq("id"),"left")
      .writeStream
      .outputMode("update")
      .format("console")
      .start().awaitTermination()
  }
}
