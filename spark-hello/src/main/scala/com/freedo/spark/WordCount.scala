package com.freedo.spark

import org.apache.spark.sql.SparkSession

object WordCount {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("SparWordCount")
      .getOrCreate();

    val sc = sparkSession.sparkContext
    val lines = sc.textFile("D:\\技术\\hello.txt")
    val wordCount = lines.flatMap(line=>line.split(" "))
      .map((_,1))
      .reduceByKey((a,b)=>a+b)
    // 由于数据分布在各个机器 使用collect 到此机器端进行展示
      wordCount.collect()
      wordCount.foreach(println)
  }


}
