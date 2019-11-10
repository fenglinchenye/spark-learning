package com.freedo.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object RDDQueueStreamingTest {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("TestRDDQueue").setMaster("local[2]")
    // 2s 重处理一次
    val ssc =new StreamingContext(sparkConf,Seconds(2))
    // 生成一个RDD队列
    val rddQueue = new mutable.SynchronizedQueue[RDD[Int]]()
    val queueStream = ssc.queueStream(rddQueue)
//    每个整型出现次数
    val mappedStream = queueStream.map(r=>(r%10,1))
    val reduceStream = mappedStream.reduceByKey(_+_)
    reduceStream.print()
    ssc.start()
    for (i<-1 to 10){
      // 不断向其中进行加入队列值
      rddQueue+=ssc.sparkContext.makeRDD(1 to 100,2)
      Thread.sleep(1000)
    }
    ssc.stop()
  }

}
