package com._51doit.spark.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Testacc1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.getSimpleName)
    val sc: SparkContext = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.makeRDD(Array(11,22,33,44,55,66,2,3),2)
    var cnts:Int=0
    rdd1.foreach(t=>{
      cnts+=1
      println(s"计数器=$cnts")
    })
   // println(cnts)
//定义一个计数器
    val acc: LongAccumulator = sc.longAccumulator("valuecnts")
    rdd1.foreach(t=>acc.add(1))
    //dd1.foreach(t=>acc.count)

    println(acc.value)
    println("------")
    println(acc.count)
    val rdd10: RDD[Int] = sc.makeRDD(Array[Int](1,2,3,4,5,6))
    //rdd10.filter()

  }

}
