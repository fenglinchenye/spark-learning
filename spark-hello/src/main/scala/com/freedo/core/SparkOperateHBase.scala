package com.freedo.core

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf}
import org.apache.spark.sql.SparkSession

object SparkOperateHBase {

  def main(args: Array[String]): Unit = {
    val conf = HBaseConfiguration.create()
    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Spark HBase")
      .config(new SparkConf())
      .getOrCreate()
    val sc = sparkSession.sparkContext

    val tablename = "student"
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE,tablename)
    val job = new Job(sc.hadoopConfiguration)
    // 设置作业
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[org.apache.hadoop.hbase.client.Result])
//    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    // 构建RDD 然后去写入
    val indataRDD = sc.makeRDD(Array("3,Rongcheng,M,26","4,GuangHua,M,27"))
    val rdd = indataRDD.map(_.split(",")).map{arr=>{
      val put = new Put(Bytes.toBytes(arr(0)))
      put.add(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))
      put.add(Bytes.toBytes("info"),Bytes.toBytes("gender"),Bytes.toBytes(arr(2)))
      put.add(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(arr(3)))
      (new ImmutableBytesWritable,put)
    }}
    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())

    // 设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE,"student")
    // 使用spark 内置的操作HBase
    val stuRDD = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[org.apache.hadoop.hbase.client.Result])
    val count = stuRDD.count()//检查记录行数
    println("Students RDD count")
    stuRDD.cache()// 持久化到内存 只生成一次不会反复执行
    // 遍历操作 (_,result) 用于匹配。只关心result
    stuRDD.foreach({case(_,result)=>
      //取出行键
        val key = Bytes.toString(result.getRow)
      // getValue()
        val name = Bytes.toString(result.getValue("info".getBytes,"name".getBytes))
        val gender = Bytes.toString(result.getValue("info".getBytes,"gender".getBytes))
        val age = Bytes.toString(result.getValue("info".getBytes,"age".getBytes))
        println("Row key:"+key+"Name:"+name+"Gender:"+gender+"Age:"+age)
    })
  }
}
