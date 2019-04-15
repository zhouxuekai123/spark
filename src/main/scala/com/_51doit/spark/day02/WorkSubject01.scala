package com._51doit.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WorkSubject01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.getSimpleName)
    val sc: SparkContext = new SparkContext(conf)
    val data: RDD[String] = sc.textFile("F:\\四期java学习视频\\辉哥视屏\\spark\\day02-spark\\spark-day02\\作业\\teacher.log")
    //data.foreach(println)
    //http://bigdata.51doit.cn/laozhang
    //http://bigdata.51doit.cn/laozhao
    //http://bigdata.51doit.cn/laozhao
    //http://bigdata.51doit.cn/laozhao
    //http://bigdata.51doit.cn/laozhao
    //获取数据 学科,姓名,整行数据
    val rdd1: RDD[(String, String, String)] = data.map(str => (str.split("\\.")(0).substring(7), str.split("/")(3), str))
    //rdd1.foreach(println)
    //学科和姓名作为key,value是1
    val rdd2: RDD[(String, Int)] = rdd1.map(str => (str._1 + "," + str._2, 1))
    //归并操作,获取k-->学科,姓名  v-->数量
    val rdd3: RDD[(String, Int)] = rdd2.reduceByKey(_ + _)
    //对结果进行排序,降序
    val rdd4: RDD[(String, Int)] = rdd3.sortBy(-_._2)
    //打印结果,因为foreach的原因,直接用rdd4.foreach(println)打印看不出来效果
    //println(rdd4.collect.toList)

    /*//按照学科进行分组
    //学科作为key,老师姓名,1作为value
    val r2: RDD[(String, (String, Int))] = rdd1.map(str=>(str._1,(str._2,1)))
    //对学科进行分组
    val r3: RDD[(String, Iterable[(String, Int)])] = r2.groupByKey()
    //对老师进行排序
    val unit: RDD[Map[String, Iterable[(String, Int)]]] = r3.map(t => {
      t._2.reduce()
    })*/
    //<学科,姓名>作为key,数量作为v
    val r2: RDD[((String, String), Int)] = rdd1.map(str => ((str._1, str._2), 1))
    //归并 操作  获取结果数据
    val r3: RDD[((String, String), Int)] = r2.reduceByKey(_ + _)
    //对结果数据进行重新的排序
    val r4: RDD[(String, (String, Int))] = r3.map(t => (t._1._1, (t._1._2, t._2)))
    //先按照学科分组
    val r5: RDD[(String, Iterable[(String, Int)])] = r4.groupByKey()
    //最后按照老师排序
    val r6: RDD[(String, List[(String, Int)])] = r5.map(t => {
      //迭代器转集合,在对老师进行排序
      val ls: List[(String, Int)] = t._2.toList.sortBy(tp => -tp._2)
      //返回数据(学科,排序好的List集合)
      (t._1, ls)
    })
    //打印结果
    println(r6.collect.toList)


  }

}
