package com._51doit.spark.day01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object test123 {
  def main(args: Array[String]): Unit = {
    var line=List("hello,nihao,lala,lala,lala","hello,nihao,lala,lala,lala")
    //scala语言
    val result: Map[String, Int] = line.flatMap(_.split(",")).map((_,1)).groupBy(_._1).mapValues(_.size)
    println(result)
    //spark计算
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("11")
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[String] = sc.makeRDD(line)
    val rdd2: RDD[(String, Int)] = rdd.flatMap(_.split(",")).map((_,1)).reduceByKey(_+_)
    rdd2.foreach(println)
  }
}
