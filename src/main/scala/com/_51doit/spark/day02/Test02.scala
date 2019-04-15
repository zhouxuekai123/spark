package com._51doit.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test02 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    val sc: SparkContext = new SparkContext(conf)

    val res1: RDD[(String, Int)] = sc.makeRDD(List(("w",1),("ac",1010),("w",100),("ab",1010)))

    val res2: RDD[(String, Iterable[Int])] = res1.groupByKey()
    val res3: RDD[(String, Int)] = res2.mapValues(_.sum)

  }
}
