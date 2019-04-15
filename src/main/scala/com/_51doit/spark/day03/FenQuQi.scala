package com._51doit.spark.day03

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object FenQuQi {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.getSimpleName)
    val sc: SparkContext = new SparkContext(conf)
    val data: RDD[String] = sc.textFile("F:\\四期java学习视频\\辉哥视屏\\spark\\day02-spark\\spark-day02\\作业\\teacher.log")
    //对数据进行切分处理
    //http://bigdata.51doit.cn/laozhao
    //http://bigdata.51doit.cn/laoduan
    val splitData: RDD[((String, String), Int)] = data.map(str => {
      val arr: Array[String] = str.split("/+")
      val tname: String = arr(2)
      val subject: String = arr(1).split("\\.")(0)
      ((subject, tname), 1)
    })
    //获取学科的数组
  val subArr: Array[String] = splitData.map(_._1._1).distinct().collect().toArray

    //调用reduceByKey方法,传入自定义的分区器,
    //把每个分区内的数据进行局部聚合,分发后再聚合   每个分区内的数据都是学科一样的
      val result: RDD[((String, String), Int)] = splitData.reduceByKey(MyisPartition(subArr),_+_)
    //对每个分区内的数据进行排序,排序是整体的事情,直接迭代一个分区内的数据,进行排序
    val result2: RDD[((String, String), Int)] = result.mapPartitions(it => {
      //迭代集合没有排序的方法,先转list集合排序,在转回城迭代器类型的返回值
      it.toList.sortBy(-_._2)/*.take(2)*/.iterator
    })
    result2.coalesce(1).foreach(println)




  }
}
//写一个伴生对象,省去了调用时候的new的过程
object MyisPartition{
  //重写apply方法
  def apply(subArr: Array[String]): MyisPartition = new MyisPartition(subArr)
}


class MyisPartition(subArr:Array[String]) extends Partitioner{
   val map: Map[String, Int] = subArr.zipWithIndex.toMap

  //分区的数量,按照学科的数量进行分区
  override def numPartitions: Int = subArr.length
//映射关系,根据学科能够获取分区的编号,map集合
  //
  override def getPartition(key: Any): Int = {
    //转换数据类型,这里的key是我们调用reduceByKey的splitData的key
    //通过key获取到学科
    val sub: String = key.asInstanceOf[(String,String)]._1
    //返回学科对应的分区编号
    map(sub)
  }

}