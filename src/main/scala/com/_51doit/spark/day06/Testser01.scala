package com._51doit.spark.day06

import org.apache.spark.{SparkConf, SparkContext}

object Testser01 {
  private val conf: SparkConf = new SparkConf()
    .set("spark.serializer","org.apache.spark.serializer.KroySerializer")
  conf.registerKryoClasses(Array(classOf[abc]))
  private val sc: SparkContext = new SparkContext(conf)


}

class abc{

}
