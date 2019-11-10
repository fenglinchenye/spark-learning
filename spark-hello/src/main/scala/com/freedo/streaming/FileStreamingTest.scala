package com.freedo.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileStreamingTest {

  def main(args: Array[String]): Unit = {

    val sparkSession =SparkSession.builder().appName("FileStreaming").master("local[2]").getOrCreate()
    val sc = sparkSession.sparkContext
    val ssc = new StreamingContext(sc,Seconds(20))
    //1、输入Dstream定义输入源
    val lines = ssc.textFileStream("D:\\技术\\log")
    // 按照空格拆分
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x=>(x,1)).reduceByKey(_+_)
    wordCounts.print()
      // 启动服务
    ssc.start()
    // 遇到错误时终止服务，否则不断的进行工作
    ssc.awaitTermination()
  }
}
