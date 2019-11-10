package com.freedo.streaming

import java.io.PrintWriter
import java.net.ServerSocket
import java.util.Random

import scala.io.Source

object DataSourceSocket {

  def index(length: Int): Int ={
    val rdm = new Random()
    rdm.nextInt(length)
  }

  def main(args: Array[String]): Unit = {

    val fileName = "D:\\技术\\log\\log.txt"
    val lines = Source.fromFile(fileName).getLines().toList
    val rowCount = lines.length
    // 绑定端口号
    val listener = new ServerSocket(9999)
    while (true){
        // 让自己进入阻塞状态，等待连接
      val socket= listener.accept()
      new Thread(){
        override def run(): Unit = {
          println("Got client connected from: "+ socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream,true)
          // 不断的发送数据
          while (true){
            // 发送间隔时间(毫秒数)
            Thread.sleep(1000.toLong)
            // 随机往外扔数据
            val content = lines(index((rowCount)))
            println(content)
            out.write(content+"\n")
            out.flush()
          }
          // 发送完成 关闭socket
          socket.close()
        }
          //运行服务端监听
      }.start()
    }
  }

}
