package com.normanlimxk.sparkworkshop

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    lazy val spark: SparkSession = {
      SparkSession.builder().
        config("spark.executor.memory", "1g").
        config("spark.driver.memory", "2g").
        master("local[1]").
        appName("Norman's Spark Program").
        getOrCreate()
    }
    val l = spark.sparkContext.parallelize((1 to 10).toList)
    l.collect().foreach(println)
  }
}
