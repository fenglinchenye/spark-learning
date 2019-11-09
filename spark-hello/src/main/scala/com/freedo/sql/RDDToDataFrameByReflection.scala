package com.freedo.sql

import org.apache.spark.sql.SparkSession

/**
  * 使用反射的方式进行获取DataFrame
  */
object RDDToDataFrameByReflection {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("DataToFrame").getOrCreate()
    // 导入对应的 隐式转换 spark 指上面的属性值
    import spark.implicits._

    val peopleDF = spark.sparkContext.textFile("D:\\技术\\people.txt")
      .map(_.split(","))
      // 因为case class 非常特殊 自动定义伴生对象 通过定义伴生对象定义一个工厂方法。不需要new
      .map(attributes=>Person(attributes(0),attributes(1).trim.toInt))
      .toDF()

    //将DataFrame 注册为一个临时表
    // 必须注册为临时表才能供下面的查询使用
    peopleDF.createOrReplaceTempView("people")

    // 最终查询出的是一个RDD
    val personsRDD = spark.sql("select name,age from people where age>20")

    personsRDD.map(t=>"Name:"+t(0)+"Age:"+t(1)).show()
  }


}
