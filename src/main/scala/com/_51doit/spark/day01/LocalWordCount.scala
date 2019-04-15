package com._51doit.spark.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LocalWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage :com._51doit.spark.day01.WordCount <input> <output>")
      sys.exit(1)
    }
    //接收参数
    val Array(input, output) = args
    //创建一个SparkContext对象
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.getSimpleName)
    //LocalWordCount$
    val sc: SparkContext = new SparkContext(conf)
    //读取文件创建RDD
    val lines: RDD[String] = sc.textFile(input)
    //切割单词
    val words: RDD[String] = lines.flatMap(x => x.split(" "))
    //组装元素
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    //分组聚合
    val reduceRDD: RDD[(String, Int)] = wordOne.reduceByKey((a, b) => a + b)
    //结果导入到hdfs中
    reduceRDD.saveAsTextFile(output)
    //关闭资源
    sc.stop()


  }

}
