package com.freedo.core

import org.apache.spark.sql.SparkSession

object WordProduce {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder
      .master("local")
      .appName("SparWordCount")
      .getOrCreate

    val sc = sparkSession.sparkContext
    val d1 = sc.parallelize(Array(("c",8),("b",25),("c",17),("a",42),("b",4),("d",9)))
    val reduceRDD = d1.reduceByKey(_+_)
    // 按照Key 的降序进行排列 false：代表降序
    val sortByKeyRDD = reduceRDD.sortByKey(false).collect
    sortByKeyRDD.foreach(println)
    // 按照value 值进行倒序排列
    val sortByValueRDD = reduceRDD.sortBy(_._2,false).collect
    sortByValueRDD.foreach(println)
//    sparkSession.close()

    val pairRDD = sc.parallelize(Array(("Hadoop",1),("Hive",1),("Spark",1),("HBase",1),("Spark",3)))
    pairRDD.mapValues(x=>x+1).foreach(println)

    val pairRDD1 = sc.parallelize(Array(("Spark","fast")))
    pairRDD.join(pairRDD1).foreach(println)

    val arrBook = sc.parallelize(Array(("hadoop",3),("spark",4),("hbase",5),("spark",8),("zookeeper",8)))
    val sumRDD = arrBook
      // 转换结果用于计数 ("spark",4)转换为("spark",(4,1))
      .mapValues((_,1))
      // 按照key 进行计算 得到类似("spark",(12,2))
      .reduceByKey((a,b)=>(a._1+b._1,a._2+b._2))
    sumRDD.foreach(println)
    // 计算平均值
    val averageRDD = sumRDD.mapValues(x=>x._1/x._2)
    averageRDD.foreach(println)
  }

}
