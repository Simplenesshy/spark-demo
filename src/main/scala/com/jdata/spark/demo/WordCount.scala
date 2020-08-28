package com.jdata.spark.demo

import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]) : Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("word count")
      .getOrCreate()
    val wordcount = spark.sparkContext.textFile("/home/simplenesshy/error.txt")
      .flatMap(_.split(" "))
      .map(x => (x, 1))
      .reduceByKey(_ + _)
    wordcount.collect().foreach(println)

  }
}
