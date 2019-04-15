package com._51doit.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object KeyWord {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)
    val sc: SparkContext = new SparkContext(conf)

    val data: RDD[String] = sc.textFile("F:\\四期java学习视频\\辉哥视屏\\spark\\day03-spark\\spark-day03\\作业\\impclick.txt")
    //data.foreach(println)
    //1017,剧情|爱情|剧情|剧情,1,1
    //1010,剧情|剧情|家庭剧|类型|热血,1,1

    //切分数据,获取需要的数据格式  1010,剧情,1,1
    val dataMap: RDD[((String, String), (Int, Int))] = data.flatMap(str => {
      val arr: Array[String] = str.split(",")
      val words: Array[String] = arr(1).split("\\|")
      words.map(word => {
        ((arr(0), word ), (arr(2).toInt, arr(3).toInt))
      })
    })
    //对数据进行递归求和
    val result: RDD[((String, String), (Int, Int))] = dataMap.reduceByKey((a, b)=>(a._1+b._1,a._2+b._2)).sortByKey()
    //result.foreach(println)

    val result2: RDD[String] = result.map(t => {
      t._1._1 + "," + t._1._2 + "," + t._2._1 + "," + t._2._1
    })
    result2.collect().foreach(println)








  }

}
