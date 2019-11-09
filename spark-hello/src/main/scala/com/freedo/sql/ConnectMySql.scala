package com.freedo.sql

import java.util.Properties

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Spark 连接 MySQL 进行分析
  */
object ConnectMySql {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("ConnectMySql").getOrCreate()
    import spark.implicits._
    // ==================================建立连接 读取数据===============================
    val jdbcDF = spark.read.format("jdbc")
      .option("url","jdbc:mysql://localhost:3306/spark")
      .option("driver","com.mysql.jdbc.Driver")
      .option("dbtable","student")
      .option("user","root")
      .option("password","123456")
      .load()
    jdbcDF.createTempView("student")
    val dataRDD = spark.sql("select * from student")
    dataRDD.map(data=>"ID: "+data(0)+", name: "+data(1)).show()

    //=======================================写数据=======================================
    // 设置模式信息
    // 封装到List 或 Array 然后赋值到StructType
    val schema = StructType(List(StructField("id",IntegerType,true),StructField("name",StringType,true),StructField("sex",StringType,true),StructField("age",IntegerType,true)))
    val students = Array("3 Rongcheng M 26","4 Guanhua M 27")
    // 创建ROW 对象，每个ROW为rowRDD中的一行
    val studentsRDD = spark.sparkContext.parallelize(students)
      .map(_.split(" "))
      .map(p=>Row(p(0).toInt,p(1).trim,p(2).trim,p(3).toInt))
    // 建立起Row 对象与模式之间的关系。把数据与模式关系对应起来
    val studentDF = spark.createDataFrame(studentsRDD,schema)

    // 创建Prop 变量用于保存JDBC 连接参数
    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","123456")
    prop.put("driver","com.mysql.jdbc.Driver")
    // 写入数据库采用追加 append 模式
    studentDF.write.mode("append").jdbc("jdbc:mysql://localhost:3306/spark","spark.student",prop)

  }

}
