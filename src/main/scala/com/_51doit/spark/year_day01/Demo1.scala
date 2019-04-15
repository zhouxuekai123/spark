package com._51doit.spark.year_day01

import com.google.gson.Gson
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Demo1 {
  def main(args: Array[String]): Unit = {
    //1.成交金额

    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.getSimpleName)
    val sc: SparkContext = new SparkContext(conf)
    //读取数据
    val lines: RDD[String] = sc.textFile("F:\\四期java学习视频\\年前星哥补课视屏\\day01\\数据和需求\\access-new.log")
    val rdd1: RDD[String] = lines.filter(_.startsWith("{"))


    val rdd2: RDD[MyMessage] = rdd1.map(str => {
      var gson: Gson = new Gson()
      var myBean: MyMessage = null
      try {
        myBean = gson.fromJson(str, classOf[MyMessage])
      } catch {
        case e: Exception => //不做任何处理
      }
      myBean
    })
    rdd2.foreach(println)

    /*
    1.成交金额
2.各个省的成交金额
3.各个省下市成交金额的TopN
4.各个支付渠道的成功支付次数和支付金额
5.各个支付渠道的支付成功率
6.各个商品分类的成交金额
7.各个分类下商品成交金额TopN（手机：1.Iphone x， 2华为P10）
8.以省份纬度统计各个分类下商品成交金额TopN（手机：1.Iphone x， 2华为P10）
     */


  }

}
