package com.freedo.sql

import org.apache.spark.sql.SparkSession

object ReadCSVToDataFrame {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local").appName("SparkSQL").getOrCreate()
    val sourceDf = sparkSession.read.csv("D:\\迅雷下载\\ml-latest-small\\tags.csv")
    println(sourceDf.show())
    println(sourceDf.printSchema())
    // 重命名
    println(sourceDf.select(sourceDf("_c0").as("userId"),sourceDf("_c1").as("movieId")).show())
    // 排序
    println(sourceDf.sort(sourceDf("_c0").desc,sourceDf("_c1").asc).show())
    sparkSession.close()
  }
}
