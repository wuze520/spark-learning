package com.wuze

import org.apache.spark.{SparkConf, SparkContext}

object Hello {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hello").setMaster("local")
    val sc = new SparkContext(conf)
    val input = args(0)
    val output = args(1)
    val aa = sc.textFile(input)
    val wc=aa.flatMap(line =>line.split(",")).map(x=>(x,1)).reduceByKey(_+_);
    //控制台输出结果
    wc.collect().foreach(println)
    val cc = aa.saveAsTextFile(output)
  }

}
