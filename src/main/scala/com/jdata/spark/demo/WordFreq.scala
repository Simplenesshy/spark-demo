package com.jdata.spark.demo
import org.apache.spark.sql.SparkSession

object WordFreq {
  def main(args: Array[String]): Unit = {
   val spark = SparkSession
      .builder()
      .appName("word freq")
      .master("local")
      .getOrCreate()


    val txtFile = "/home/simplenesshy/error.txt";
    val txtData = spark.read.textFile(txtFile)
    txtData.show()
    spark.stop()
  }
}