package com.freedo.core

import org.apache.spark.sql.SparkSession

/**
  * Join 查询
  * 数据来源：https://grouplens.org/datasets/movielens/
  */
object SparkJoin {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("SparkJoin").master("local").getOrCreate()
    val sc = sparkSession.sparkContext
    // 比分RDD
    val ratingsRDD = sc.textFile("D:\\迅雷下载\\ml-latest-small\\ratings.csv")
    // 计算总值
    val reduceRDD = ratingsRDD.filter(_.trim.length>0).map(line=>(line.split(",")(1),(line.split(",")(2).toDouble,1))).reduceByKey((a,b)=>(a._1+b._1,a._2+b._2))
    // 做平均处理
    val averageRDD = reduceRDD.map(a=>(a._1,a._2._1./(a._2._2)))
    // 引入基础数据源
    val movieRDD = sc.textFile("D:\\迅雷下载\\ml-latest-small\\movies.csv")
    val movieSourceRDD = movieRDD.map(line=>(line.split(",")(0),line.split(",")(1)))
    // 左外连接后进行倒序排列
    val fullSourceRDD = averageRDD.leftOuterJoin(movieSourceRDD).filter(a=>a._2._1>4).sortBy(a=>a._2._1,false)
    fullSourceRDD.foreach(println)
  }
}
