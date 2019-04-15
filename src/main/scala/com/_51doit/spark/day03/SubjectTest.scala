package com._51doit.spark.day03

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SubjectTest {
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
          //获得老师姓名就是
          val tname = arr(2)
          //获得学科名字
          val subject = arr(1).split("\\.")(0)
          //返回需要的数据
          ((subject, tname), 1)
        })
    //获取学科数组,作为参数,传入自定义的分区器
    val subData: Array[String] = splitData.map(_._1._1).collect().toArray.distinct

    //调用reduceByKey方法,传入自定义的分区器
    val result: RDD[((String, String), Int)] = splitData.reduceByKey(MyPartition(subData),_+_)

    //对结果数据进行组内排序,每一个分区都是一个学科的所有老师的浏览次数
    //因为是一个分区一组数据,所以不能用map,应该用mapPartitions
    val result2: RDD[((String, String), Int)] = result.mapPartitions(it => {
      //一次map的是一个分区的数据,是一个迭代器集合,所以需要转list集合排序,在转回迭代器集合
      it.toList.sortBy(-_._2).iterator
    })
    println(result2.partitions.size)

    result2.coalesce(1).foreach(println)

  }

}
//写一个分区器的伴生对象,重写apply方法,就可以直接new对象,调用就可以直接调用了
object MyPartition{
  def apply(subData: Array[String]): MyPartition = new MyPartition(subData)
}
//定义一个类,继承partition
class MyPartition(subData:Array[String]) extends Partitioner {
  //d定义一个map,用来key和分区编号做映射关系
   val subWithIndexMap: Map[String, Int]= subData.zipWithIndex.toMap

  //方法一:分区数量
  override def numPartitions: Int = subData.length

  //方法二: 根据key获取对应的分区编号,key的准确类型是(String, String)
  override def getPartition(key: Any): Int = {
    //因为参数是Any类型的,需要将类型转换成(String, String)类型
    val sub: String = key.asInstanceOf[(String,String)]._1
    //map的映射关系,根据学科,返回分区编号
    subWithIndexMap(sub)
  }
}
