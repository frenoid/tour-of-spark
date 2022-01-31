package com.normanlimxk.sparkworkshop

import org.apache.spark.sql.{SparkSession, types}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

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

    val customerSchema = new StructType().
      add("CustomerID", IntegerType).
      add("CustomerName", StringType).
      add("MemCat", StringType).
      add("Age", IntegerType).
      add("Gender", StringType).
      add("AmtSpent", DoubleType).
      add("Address", StringType).
      add("City", StringType).
      add("CountryID", StringType).
      add("Title", StringType).
      add("PhoneNo", StringType)

    val customer = spark.
      read.
      option("header", "true").
      schema(customerSchema).
      csv("src/main/resources/customer.csv")

    customer.show()
    customer.printSchema()
  }
}
