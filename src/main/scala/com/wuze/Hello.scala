package com.wuze

import org.apache.spark.{SparkConf, SparkContext}

object Hello {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hello").setMaster("local")
    val sc = new SparkContext(conf)
    val input = "C:\\test\\test.txt"
    val output = "C:\\test\\test"
    val aa = sc.textFile(input)
//    val bb = aa.map(x => {
//      val pair = x.split(",")
//      ((pair(0)),(pair(1)))
//      println("pair(0) :" + pair(0) + "pair(1) :" + pair(1) )
//    })
    val wc=aa.flatMap(line =>line.split(",")).map(x=>(x,1)).reduceByKey(_+_);
//试试git提交功能
    //控制台输出结果
    wc.collect().foreach(println)
    val cc = aa.saveAsTextFile(output)

  }

}
