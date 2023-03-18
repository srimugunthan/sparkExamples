package com.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.tools.nsc.transform.Flatten
import org.apache.spark.rdd.RDD

object WordCount {

  def main(args: Array[String]) = {
    println("Hello, " + args(0))
    val conf = new SparkConf().setAppName("HelloWorld").setMaster("local")
    val context: SparkContext = new SparkContext(conf)
    val filename : String = "hdfs://hadoop-host-1:8020/" + args(0)
    val textFile: RDD[String] = context.textFile(filename, 1)
    val tokenized = textFile.flatMap(line => line.split(" "))
    val wordCounts = tokenized.map(word => (word, 1))
    val count = wordCounts.reduceByKey((a,b) => a + b)
    count.saveAsTextFile("scalaoutput")
    println(count.collect.mkString(", "));
    
    context.stop
  }

}