package com.freedo.spark

import org.apache.spark.sql.SparkSession

object TopN {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().master("local").appName("TopN").getOrCreate()
    val sc = sparkSession.sparkContext
    // 设置日志输出级别
    sc.setLogLevel("ERROR")
    // 把目录下的文件加载进来，分区
    val lines = sc.textFile("D:\\技术\\hello.txt",2)
    var num = 0
    lines.filter(line=>line.trim.length>0&&line.split(",").length==4)
      .map(_.split(",")(2))
      .map(x=>(x.toInt,""))
      .sortByKey(false)
      .map(x=>x._1).take(5)
      .foreach(x=>{
        num = num + 1
        println(num+"\t"+x)
      })
  }
}
