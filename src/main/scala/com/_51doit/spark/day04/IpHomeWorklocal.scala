package com._51doit.spark.day04

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IpHomeWorklocal {
  //定义一个方法,二分查找 方便调用参数有两个  一个是ip的long值,一个是大表的数组
  def finds(ip:Long,arr:Array[(Long,Long,String)]):(String,Int)={
    var start:Int=0
    var end:Int=arr.length-1
    //进行二分查找
    while (start <= end){
      var  middle:Int=(start+end)/2
      if (ip>=arr(middle)._1 && ip<=arr(middle)._2){
        //找到了就返回省份和1
        return (arr(middle)._3,1)
      }else if (ip < arr(middle)._1){
        end =middle-1
      }else if(ip > arr(middle)._2){
        start=middle+1
      }
    }
    //没找到返回 NULL,1
    return ("NULL",1)
  }




  def main(args: Array[String]): Unit = {
    //val Array(logs,ip)=args
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.getSimpleName)
    val sc: SparkContext = new SparkContext(conf)
    //读取日志文件
   val logdata: RDD[String] = sc.textFile("F:\\四期java学习视频\\辉哥视屏\\spark\\day04-spark\\spark-day04\\作业题\\ipaccess.log")
    //val logdata: RDD[String] = sc.textFile(logs)
    //读取ip区间对应的信息文件
    val data: RDD[String] = sc.textFile("F:\\四期java学习视频\\辉哥视屏\\spark\\day04-spark\\spark-day04\\作业题\\ip.txt")
    //val data: RDD[String] = sc.textFile(ip)

    //处理文件,获取我们能够拿着ip就来查信息的大表数据
    val data1: RDD[(Long, Long, String)] = data.map(str => {
      //1.2.5.0|1.2.7.255|16909568|16910335|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
      val arr: Array[String] = str.split("\\|")
      var startIp: Long = 0
      var endIP: Long = 0
      var province: String = null
      try {
        startIp = arr(2).toLong
        endIP = arr(3).toLong
        province = arr(6)
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
      //返回每条信息  起始ip   终止ip   省份
      (startIp, endIP, province)
    })
    //获取到我们需要的集合数据,为二分查找准备
    val data2: Array[(Long, Long, String)] = data1.collect()
    //data2.foreach(println)

    //获取我们需要查询的所有的ip数据
    val logIPdata: RDD[Long] = logdata.map(str => {
      val split: Array[String] = str.split("\\|")
      //ip地址转为long值,看成256进制
      val ip: Array[String] = split(1).split("\\.")
      val a: Long = ip(0).toLong * 256 * 256 * 256
      val b: Long = ip(1).toLong * 256 * 256
      val c: Long = ip(2).toLong * 256
      val d: Long = ip(3).toLong
      //返回ip的long值
      a + b + c + d
    })
   //logIPdata.foreach(println)


    //logIPdata的每条数据都去data2中查询我们所需要的数据.调用二分查找方法
    val resultOne: RDD[(String, Int)] = logIPdata.map(ip => {
      val str: (String, Int) = finds(ip, data2)
      str
    })
    val result: RDD[(String, Int)] = resultOne.reduceByKey(_+_)
    //result.foreach(println)

    // 把结果数据写入到mysql中  collect




    result.foreachPartition(it => {
      // 获取mysql连接
      val url = "jdbc:mysql://192.168.133.2:3306/demo1?characterEncoding=utf-8"

      var conn: Connection = null
      var pstm1: PreparedStatement = null
      var pstm2: PreparedStatement = null
      try {

        conn = DriverManager.getConnection(url, "root", "123456")

        pstm1 = conn.prepareStatement("create table if not exists province (province varchar(30),cnts int)")

        pstm1.execute()

        pstm2 = conn.prepareStatement("insert into province values(?,?)")
        it.foreach { case (pro, cnt) => {
          pstm2.setString(1, pro)
          pstm2.setInt(2, cnt)
          pstm2.execute()
        }
        }
      } catch {
        // 打印错误
        case e: Exception => e.printStackTrace()
      } finally {
        IPUtils(conn, pstm1, pstm2)
      }
    })

    sc.stop()
  }
  private def IPUtils(conn: Connection, pstm1: PreparedStatement, pstm2: PreparedStatement) = {
    if (pstm1 != null) pstm1.close()
    if (pstm2 != null) pstm2.close()
    if (conn != null) conn.close()
  }

}
