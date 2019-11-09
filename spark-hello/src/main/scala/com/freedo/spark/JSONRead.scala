package com.freedo.spark

import org.apache.spark.sql.SparkSession

import scala.util.parsing.json.JSON

/**
  * JSON 解析
  */
object JSONRead {

  def main(args: Array[String]): Unit = {

    val inputFile = "D:\\logs\\people.json"
    val sparkSession = SparkSession.builder()
                      .master("local")
                      .appName("Spark Read Json")
                      .getOrCreate()
    val sc = sparkSession.sparkContext
    val jsonStrs = sc.textFile(inputFile)
    //解析 JSON 对象解析成功时会将数据封装到Some(map:Map[String,Any]) 失败了会封装到None 对象
    val result = jsonStrs.map(s=>JSON.parseFull(s))
    result.foreach({r => r match {
      case Some(map: Map[String,Any])=>println(map)
      case None=>println("Parsing failed")
      case other=>println("Unknown data structure:"+other)
    }})
  }
}
