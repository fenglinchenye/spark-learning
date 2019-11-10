package com.freedo.core

import org.apache.spark.sql.SparkSession

object SecondarySortKeyTest {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().master("local").appName("SecondaryKeySort").getOrCreate()
    val sc = sparkSession.sparkContext
    val lines = sc.textFile("D:\\技术\\secondsort.txt")
    val rdd = lines.map(line=>(new SecondarySortKey(line.split(",")(0).toInt,line.split(",")(1).toInt),line))
      .sortByKey(true).map(_._2).collect()
    rdd.foreach(
      println
    )
  }


}
