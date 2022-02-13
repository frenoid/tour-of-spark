package com.normanlimxk.sparkworkshop

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import com.mysql.jdbc.exceptions.jdbc4.CommunicationsException

object Tutorial extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    val customerFilePath = "src/main/resources/Customer.csv"
    val countryFilePath = "src/main/resources/Country.csv"
    val countryOutPath = "src/main/resources/out/Country"

    // Example 1: Retrieving all data from Customers Dataset
    val customerSchema = new StructType()
      .add("CustomerID", IntegerType)
      .add("CustomerName", StringType)
      .add("MemberCategory", StringType)
      .add("Age", IntegerType)
      .add("Gender", StringType)
      .add("AmountSpent", DoubleType)
      .add("Address", StringType)
      .add("City", StringType)
      .add("CountryID", StringType)
      .add("Title", StringType)
      .add("PhoneNo", StringType)

    val df = spark.read
      .option("header", "true")
      .schema(customerSchema)
      .csv(customerFilePath)

    df.show(200, false)
    df.printSchema()

    // Example 2: Returning Selected Fields
    // There are multiple ways of doing this

    // You can use the $ notation by importing spark.implicits
    import spark.implicits._
    df.select($"CustomerID", $"CustomerName", $"Age")
      .show(200, false)

    // You can insert column names into Dataframe()
    df.select(df("CustomerID"), df("CustomerName"), df("Age"))
      .show(200, false)

    // You can input column name as string
    df.select("CustomerID", "CustomerName", "Age")
      .show(200, false)

    // You can use the column function
    import org.apache.spark.sql.functions.col
    df.select(col("CustomerID"), col("CustomerName"), col("Age"))
      .show(200, false)

    // Example 3: Retrieving data from the Customers Dataset & displaying sorted on Customer Name
    df.orderBy("CustomerName")
      .show()

    // Getting the data in descending sequence
    import org.apache.spark.sql.functions.desc
    df.orderBy(desc("CustomerName"))
      .show()

    // Example 4: Selective retrieval

    // a. Obtaining only those Customers whose MemberCategory is ‘A’
    df.filter($"MemberCategory" === "A")
      .show()

    // b. Obtaining only those Customers whose MemberCategory is ‘A’ & their name starts with ‘T’
    df.filter($"MemberCategory" === "A" && $"CustomerName".like("T%"))
      .show()

    // Example 5: Combining functions
    df.filter($"MemberCategory" === "A")
      .orderBy("CustomerName")
      .show(300, false)

    // Example: 6 Getting a count
    val count = df.filter($"MemberCategory" === "A").count()
    println(count)

    // Example 7: Getting the Sum of a field
    import org.apache.spark.sql.functions.sum
    val totalAmountSpent = df.agg(sum($"AmountSpent")).first()(0)
    println(totalAmountSpent)

    // Example 8: Getting the Sum of a field with condition on the rows to use
    val totalAmountSpentByMemCatA = df.filter($"MemberCategory" === "A").agg(sum("AmountSpent")).first()(0)
    println(totalAmountSpentByMemCatA)

    // Example 9: Getting the Average of a field
    import org.apache.spark.sql.functions.avg
    val averageAgeOfCustomers = df.agg(avg("Age")).first()(0)
    println(averageAgeOfCustomers)

    // Example 10: Grouping & Subtotals based on single parameter
    df.groupBy("MemberCategory")
      .sum("AmountSpent")
      .show(200, false)

    // Example 11: Grouping & Subtotals based on multiple parameters
    df.groupBy("MemberCategory","Gender")
      .sum("AmountSpent")
      .orderBy("MemberCategory","Gender")
      .show(200, false)

    // Example 12: ROLL UP function
    df.rollup("MemberCategory","Gender")
      .sum("AmountSpent")
      .orderBy("MemberCategory","Gender")
      .show(200, false)

    // Example 13: The CUBE function instead of ROLL UP
    df.cube("MemberCategory","Gender")
      .sum("AmountSpent")
      .orderBy("MemberCategory","Gender")
      .show(200, false)

    // Example 14: Getting the Standard Deviation of a field
    import org.apache.spark.sql.functions.stddev_pop
    val std = df.agg(stddev_pop("AmountSpent")).first()(0)
    println(std)

    // Example 15: Getting the Skewness of a field
    import org.apache.spark.sql.functions.skewness
    val skw = df.agg(skewness("AmountSpent")).first()(0)
    print(skw)

    // Example 16: Getting the Common Statistics of a few fields in one function call
    df.describe("MemberCategory", "Gender", "AmountSpent", "Age")
      .show()


    // Example 17: Joining two data sources (CSV) and retrieving data from both
    val dfCustomer = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(customerFilePath)

    val dfCountry = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(countryFilePath)

    val joinDF = dfCustomer.join(dfCountry, "CountryCode")
    joinDF.select($"CustomerID", $"CustomerName", $"CountryCode", $"CountryName", $"Currency", $"TimeZone")
      .show(300, false)

    // Example 19: Retrieving Data from MySQL Source.
    // Download mysql-connector-java from https://mvnrepository.com/artifact/mysql/mysql-connector-java/5.1.49
    // Go to File > Project Structure > + and pick the jar you downloaded to add it to the classpath
    try {
      val videoshopDF = spark.read
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/videoshop")
        .option("driver", "com.mysql.jdbc.Driver")
        .option("dbtable", "Movies")
        .option("user", "root")
        .option("password", "norman")
        .load()
      videoshopDF.show(200, false)
    } catch {
      case e: ClassNotFoundException => {
          println("Could not read from the MySQL database")
          println("Download the mysql jdbc driver from https://mvnrepository.com/artifact/mysql/mysql-connector-java/5.1.49 and add it to the classpath")
      }
      case e: CommunicationsException => {
        println("Could not connect to MySQL")
        println("Ensure there is a MySQL server running on localhost. Try Docker")
      }
    }

    // Example 20: Saving a CSV file
    dfCountry.write
      .mode("overwrite")
      .json(countryOutPath)
  }
}
