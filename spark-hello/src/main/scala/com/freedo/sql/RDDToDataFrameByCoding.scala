package com.freedo.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * 使用编程方式定义RDD
  */
object RDDToDataFrameByCoding {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Spark").master("local").getOrCreate()
    import spark.implicits._
    // 制作表头
    val fields = Array(StructField("name",StringType,true),StructField("age",IntegerType,true))
    // 定义表结构
    val schema = StructType(fields)

    // 读入数据
    val peopleRDD = spark.sparkContext.textFile("D:\\技术\\people.txt")
    // 遍历解析获取每一行的数据
    val rowRDD= peopleRDD.map(_.split(",")).map(attributes=>Row(attributes(0),attributes(1).trim.toInt))
    //把表头和表中的数据进行拼接起来
    val peopleDF = spark.createDataFrame(rowRDD,schema)
    // 创建临时表 供查询使用
    peopleDF.createOrReplaceTempView("people")
    // 查询结果
    val personsRDD = spark.sql("select name,age from people where age>20")
    personsRDD.map(t=>"Name:"+t(0)+" Age:"+t(1)).show()
  }


}
