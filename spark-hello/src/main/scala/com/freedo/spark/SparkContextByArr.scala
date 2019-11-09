package com.freedo.spark

import org.apache.spark.sql.SparkSession

object SparkContextByArr {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Spark")
      .master("local")
      .config("spark.default.parallelism",6)
      .getOrCreate()
    val sc = spark.sparkContext
    val list = List("Hadoop","Spark","Hive")
    val rdd = sc.parallelize(list)
    rdd.cache()//标记缓存
    println(rdd.count())//真正运行的时候进行缓存
    println(rdd.collect().mkString(","))//调用缓存结果
    spark.stop()
  }
}
