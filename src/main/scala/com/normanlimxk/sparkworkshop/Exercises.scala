package com.normanlimxk.sparkworkshop

import org.apache.spark.sql.functions.{col, skewness, stddev_pop, variance, round}

object Exercises extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    import  spark.implicits._

    // 1. Retrieve all data from the Movies table
    val moviesDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/Movies.csv")
    moviesDF.printSchema()
    moviesDF.show(5, false)

    // 2. Retrieve all Movies and display the data in ascending order of Movie Title.
    moviesDF
      .orderBy(col("MovieTitle").asc)
      .show(truncate = false)

    // 3. Retrieve all R rated movies. You should display only the Video Code, Movie Title, and Movie Type.
    moviesDF
      .filter($"Rating" === "R")
      .select($"VideoCode", $"MovieTitle", $"MovieType")
      .show(truncate = false)

    // 4. Retrieve all Science Fiction movies produced by Warner.
    moviesDF
      .filter($"MovieType" === "Sci-fi" && $"ProducerID" === "Warner")
      .show(truncate = false)

    // 5. Determine the average rental price of all movies. Explore the variance, standard deviation and skewness of this population
    moviesDF
      .agg(
        round(variance("RentalPrice"),2).as("Variance"),
        round(stddev_pop("RentalPrice"), 2).as("Standard Deviation"),
        round(skewness("RentalPrice"), 2).as("Skewness"))
      .show()

    // 6. Find the total stock of movies grouped by Movie Type and Rating (two fields).
    //    Use the RollUp function and build the cube function to determine the dimensional totals for the above grouping
    moviesDF
      .groupBy($"MovieType", $"Rating")
      .sum("TotalStock")
      .orderBy($"MovieType", $"Rating")
      .show()

    moviesDF.rollup($"MovieType", $"Rating")
      .sum("TotalStock")
      .orderBy($"MovieType", $"Rating")
      .show()

    moviesDF.cube($"MovieType", $"Rating")
      .sum("TotalStock")
      .orderBy($"MovieType", $"Rating")
      .show()
  }
}
