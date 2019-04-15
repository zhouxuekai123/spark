package com._51doit.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object workPVUV {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)
    val sc: SparkContext = new SparkContext(conf)
    //1读取数据
    val data: RDD[String] = sc.textFile("F:\\四期java学习视频\\辉哥视屏\\day04-scala-02\\scala-day02\\pvuv\\pvuv.txt")
    //data.foreach(println)
    //site1,user1,2018-03-01 02:12:22
    //需求1：统计 每一个网站的pv和uv (按天)        结果形式 site1,10,5
    //2过滤脏数据,得到正确的数据
    val dataRdd: RDD[String] = data.filter(str => str.length > 30)
    // dataRdd.foreach(println)
    //3处理数据,或得需要的数据形式
    val rdd1: RDD[mutable.Buffer[String]] = dataRdd.map(t => t.split(",").toBuffer)
    val rdd2: RDD[(String, String, String)] = rdd1.map(t => (t(0), t(1), t(2).substring(0, 10)))
    //rdd2.foreach(println)
    //site1,user1       site1,user1,2018-03-01   site1,user1,2018-03-01  site1,user2,2018-03-01
    val rdd3: RDD[((String, String), Iterable[(String, String, String)])] = rdd2.groupBy(str => (str._1, str._3))
    val rdd4: RDD[((String, String), (Int, Int))] = rdd3.mapValues(it => {
      val ls: Iterable[String] = it.map(tp => tp._2)
      val pv: Int = ls.size
      val uv: Int = ls.toList.distinct.size
      (pv, uv)
    })
    val result: RDD[String] = rdd4.map(t => t._1._1 + "," + t._1._2 + "," + t._2._1 + "," + t._2._2)
    result.foreach(println)


  }

}
