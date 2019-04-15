package com._51doit.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]): Unit = {
    val d1 = Array(("bj",28.1), ("sh",28.7), ("gz",32.0), ("sz", 33.1))
    val d2 = Array(("bj",27.3), ("sh",30.1), ("gz",33.3))
    val d3 = Array(("bj",28.2), ("sh",29.1), ("gz",32.0), ("sz", 30.5))

    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.getSimpleName)
    val sc: SparkContext = new SparkContext(conf)
    val dd: Array[(String, Double)] = d1++d2++d3
    val dateRdd: RDD[(String, Double)] = sc.makeRDD(dd)
    val rdd1: RDD[(String, Iterable[Double])] = dateRdd.groupByKey()
    val rdd2: RDD[(String, Double)] = rdd1.mapValues(t=>(t.sum/t.size))
    rdd2.foreach(println)
    val value: RDD[(String, Double)] = dateRdd.reduceByKey(_+_)
    val date: RDD[(String, Array[Double])] = dateRdd.mapValues(Array(_))

    val rdd3: RDD[(String, Array[Double])] = date.reduceByKey(_++_)
   // val result1: RDD[(String, Double)] = rdd3.map(t=>(t._1,t._2.sum/t._2.size))

    val result: RDD[(String, Double)] = rdd3.mapValues(t=>(t.sum/t.size))

    result.foreach(println)




  }

}
