package com.freedo.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetworkWordCount {

  def main(args: Array[String]): Unit = {

      val sparkSession = SparkSession.builder().appName("NetworkWordCount").master("local[2]")
        .getOrCreate()

      val ssc =new StreamingContext(sparkSession.sparkContext,Seconds(1))
      // 使用linux NC 程序 nc -lk 9999
      // 监听远程的socket
      val lines = ssc.socketTextStream("localhost",9999,StorageLevel.MEMORY_AND_DISK_SER)
      val words = lines.flatMap(_.split(" "))
      val wordCounts = words.map(x=>(x,1)).reduceByKey(_+_)
      wordCounts.print()
      ssc.start()
      ssc.awaitTermination()
  }
}
