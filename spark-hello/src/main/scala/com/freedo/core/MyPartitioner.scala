package com.freedo.core

import org.apache.spark.sql.SparkSession
import org.apache.spark.Partitioner
// 自定义分区数 需要继承自
class MyPartitioner(numParts:Int) extends Partitioner{

  // 覆盖分区数
  override def numPartitions: Int = numParts

  // 覆盖分区号获取
  override def getPartition(key: Any): Int = {
    key.toString.toInt%10
  }
}
object TestPartitioner{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Spark")
      .master("local")
      .config("spark.default.parallelism",6)
      .getOrCreate()
    // 获取Spark Context 对象
    val sc = spark.sparkContext
    //模拟5个分区的数据
    val data = sc.parallelize(1 to 10,5)
    // 将尾号转变为10个分区，分别写到10个文件
    // Partitioner 分区类 只支持键值对的，故要先转换到键值对的RDD、
    // 转变结果为 (1,1),(2,1)...(10,1)
    data.map((_,1))
      // 对键值对按照分区规则进行分区 将尾号转变为10个分区进行分别写到10个文件
      .partitionBy(new MyPartitioner(10))
      // 将RDD 的（key,value）的第一个元素取出。获得为1,2,3,4,5
      .map (_._1)
      // 保存在对应的文件目录，里面会包含很多的分区文件
      .saveAsTextFile("D:\\技术\\a")
  }
}