package com.freedo.spark

import org.apache.spark.sql.SparkSession

object SparkCondition {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Spark")
      .master("local")
      .config("spark.default.parallelism",6)
      .getOrCreate()
    // 获取Spark Context 对象
    val sc = spark.sparkContext
    val rdd = sc.textFile("D:\\技术\\hello.txt",2)
    val array = Array(1,2,3,4,5)
    val arrRdd = sc.parallelize(array,2)// 设置两个分区
    println(rdd.partitions.size)
    val rd1 = rdd.repartition(1)// 重分区
    println(rd1.partitions.size)
    sc.stop();

  }


}
